/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2

import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.executor.CommitDeniedException
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.streaming.{MicroBatchExecution, StreamExecution}
import org.apache.spark.sql.execution.streaming.continuous.{CommitPartitionEpoch, ContinuousExecution, EpochCoordinatorRef, SetWriterPartitions}
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

/**
 * The logical plan for writing data into data source v2.
 */
case class WriteToDataSourceV2(writer: DataSourceWriter, query: LogicalPlan) extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(query)
  override def output: Seq[Attribute] = Nil
}

/**
 * The physical plan for writing data into data source v2.
 */
case class WriteToDataSourceV2Exec(writer: DataSourceWriter, query: SparkPlan) extends SparkPlan {
  override def children: Seq[SparkPlan] = Seq(query)
  override def output: Seq[Attribute] = Nil

  override protected def doExecute(): RDD[InternalRow] = {
    val writeTask = writer match {
      case w: SupportsWriteInternalRow => w.createInternalRowWriterFactory()
      case _ => new InternalRowDataWriterFactory(writer.createWriterFactory(), query.schema)
    }

    val useCommitCoordinator = writer.useCommitCoordinator
    val rdd = query.execute()
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)

    logInfo(s"Start processing data source writer: $writer. " +
      s"The input RDD has ${messages.length} partitions.")

    try {
      val runTask = writer match {
        // This case means that we're doing continuous processing. In microbatch streaming, the
        // StreamWriter is wrapped in a MicroBatchWriter, which is executed as a normal batch.
        case w: StreamWriter =>
          EpochCoordinatorRef.get(
            sparkContext.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY),
            sparkContext.env)
            .askSync[Unit](SetWriterPartitions(rdd.getNumPartitions))

          (context: TaskContext, iter: Iterator[InternalRow]) =>
            DataWritingSparkTask.runContinuous(writeTask, context, iter)
        case _ =>
          (context: TaskContext, iter: Iterator[InternalRow]) =>
            DataWritingSparkTask.run(writeTask, context, iter, useCommitCoordinator)
      }

      sparkContext.runJob(
        rdd,
        runTask,
        rdd.partitions.indices,
        (index, message: WriterCommitMessage) => {
          messages(index) = message
          writer.onDataWriterCommit(message)
        }
      )

      if (!writer.isInstanceOf[StreamWriter]) {
        logInfo(s"Data source writer $writer is committing.")
        writer.commit(messages)
        logInfo(s"Data source writer $writer committed.")
      }
    } catch {
      case _: InterruptedException if writer.isInstanceOf[StreamWriter] =>
        // Interruption is how continuous queries are ended, so accept and ignore the exception.
      case cause: Throwable =>
        logError(s"Data source writer $writer is aborting.")
        try {
          writer.abort(messages)
        } catch {
          case t: Throwable =>
            logError(s"Data source writer $writer failed to abort.")
            cause.addSuppressed(t)
            throw new SparkException("Writing job failed.", cause)
        }
        logError(s"Data source writer $writer aborted.")
        cause match {
          // Do not wrap interruption exceptions that will be handled by streaming specially.
          case _ if StreamExecution.isInterruptionException(cause) => throw cause
          // Only wrap non fatal exceptions.
          case NonFatal(e) => throw new SparkException("Writing job aborted.", e)
          case _ => throw cause
        }
    }

    sparkContext.emptyRDD
  }
}

object DataWritingSparkTask extends Logging {
  def run(
      writeTask: DataWriterFactory[InternalRow],
      context: TaskContext,
      iter: Iterator[InternalRow],
      useCommitCoordinator: Boolean): WriterCommitMessage = {
    val stageId = context.stageId()
    val partId = context.partitionId()
    val attemptId = context.attemptNumber()
    val epochId = Option(context.getLocalProperty(MicroBatchExecution.BATCH_ID_KEY)).getOrElse("0")
    val dataWriter = writeTask.createDataWriter(partId, attemptId, epochId.toLong)

    // write the data and commit this writer.
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      iter.foreach(dataWriter.write)

      val msg = if (useCommitCoordinator) {
        val coordinator = SparkEnv.get.outputCommitCoordinator
        val commitAuthorized = coordinator.canCommit(context.stageId(), partId, attemptId)
        if (commitAuthorized) {
          logInfo(s"Writer for stage $stageId, task $partId.$attemptId is authorized to commit.")
          dataWriter.commit()
        } else {
          val message = s"Stage $stageId, task $partId.$attemptId: driver did not authorize commit"
          logInfo(message)
          // throwing CommitDeniedException will trigger the catch block for abort
          throw new CommitDeniedException(message, stageId, partId, attemptId)
        }

      } else {
        logInfo(s"Writer for partition ${context.partitionId()} is committing.")
        dataWriter.commit()
      }

      logInfo(s"Writer for stage $stageId, task $partId.$attemptId committed.")

      msg

    })(catchBlock = {
      // If there is an error, abort this writer
      logError(s"Writer for stage $stageId, task $partId.$attemptId is aborting.")
      dataWriter.abort()
      logError(s"Writer for stage $stageId, task $partId.$attemptId aborted.")
    })
  }

  def runContinuous(
      writeTask: DataWriterFactory[InternalRow],
      context: TaskContext,
      iter: Iterator[InternalRow]): WriterCommitMessage = {
    val epochCoordinator = EpochCoordinatorRef.get(
      context.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY),
      SparkEnv.get)
    val currentMsg: WriterCommitMessage = null
    var currentEpoch = context.getLocalProperty(ContinuousExecution.START_EPOCH_KEY).toLong

    do {
      var dataWriter: DataWriter[InternalRow] = null
      // write the data and commit this writer.
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        try {
          dataWriter = writeTask.createDataWriter(
            context.partitionId(), context.attemptNumber(), currentEpoch)
          while (iter.hasNext) {
            dataWriter.write(iter.next())
          }
          logInfo(s"Writer for partition ${context.partitionId()} is committing.")
          val msg = dataWriter.commit()
          logInfo(s"Writer for partition ${context.partitionId()} committed.")
          epochCoordinator.send(
            CommitPartitionEpoch(context.partitionId(), currentEpoch, msg)
          )
          currentEpoch += 1
        } catch {
          case _: InterruptedException =>
            // Continuous shutdown always involves an interrupt. Just finish the task.
        }
      })(catchBlock = {
        // If there is an error, abort this writer. We enter this callback in the middle of
        // rethrowing an exception, so runContinuous will stop executing at this point.
        logError(s"Writer for partition ${context.partitionId()} is aborting.")
        if (dataWriter != null) dataWriter.abort()
        logError(s"Writer for partition ${context.partitionId()} aborted.")
      })
    } while (!context.isInterrupted())

    currentMsg
  }
}

class InternalRowDataWriterFactory(
    rowWriterFactory: DataWriterFactory[Row],
    schema: StructType) extends DataWriterFactory[InternalRow] {

  override def createDataWriter(
      partitionId: Int,
      attemptNumber: Int,
      epochId: Long): DataWriter[InternalRow] = {
    new InternalRowDataWriter(
      rowWriterFactory.createDataWriter(partitionId, attemptNumber, epochId),
      RowEncoder.apply(schema).resolveAndBind())
  }
}

class InternalRowDataWriter(rowWriter: DataWriter[Row], encoder: ExpressionEncoder[Row])
  extends DataWriter[InternalRow] {

  override def write(record: InternalRow): Unit = rowWriter.write(encoder.fromRow(record))

  override def commit(): WriterCommitMessage = rowWriter.commit()

  override def abort(): Unit = rowWriter.abort()
}

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

package org.apache.spark.network.netty

import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.existentials
import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.protocol.{BlockTransferMessage, OpenBlocks, StreamHandle, UploadBlock}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage._


/**
 * Serves requests to open blocks by simply registering one chunk per block requested.
 * Handles opening and uploading arbitrary BlockManager blocks.
 *
 * Opened blocks are registered with the "one-for-one" strategy, meaning each Transport-layer Chunk
 * is equivalent to one Spark-level shuffle block.
 */
class NettyBlockRpcServer(
    appId: String,
    serializer: Serializer,
    blockManager: BlockDataManager)
  extends RpcHandler with Logging {

  private val streamManager = new OneForOneStreamManager()

  private def mergeContiguousShuffleBlocks(blockIds: Array[String]) = {
    logDebug(s"blockIds: ${blockIds.mkString(" ")}")
    val shuffleBlockIds = blockIds.map(id => BlockId(id).asInstanceOf[ShuffleBlockId])
    var continuousShuffleBlockIds = ArrayBuffer.empty[ContinuousShuffleBlockId]

    def sameMapFile(index: Int) = {
      shuffleBlockIds(index).shuffleId == shuffleBlockIds(index - 1).shuffleId &&
        shuffleBlockIds(index).mapId == shuffleBlockIds(index - 1).mapId
    }

    if (shuffleBlockIds.nonEmpty) {
      var prev = 0
      (0 until shuffleBlockIds.length + 1).foreach { idx =>
        if (idx != 0) {
          if (idx == shuffleBlockIds.length || !sameMapFile(idx)) {
            continuousShuffleBlockIds += ContinuousShuffleBlockId(
              shuffleBlockIds(prev).shuffleId,
              shuffleBlockIds(prev).mapId,
              shuffleBlockIds(prev).reduceId,
              shuffleBlockIds(idx - 1).reduceId - shuffleBlockIds(prev).reduceId + 1
            )
            prev = idx
          }
        }
      }
    }
    continuousShuffleBlockIds
  }

  override def receive(
      client: TransportClient,
      rpcMessage: ByteBuffer,
      responseContext: RpcResponseCallback): Unit = {
    val message = BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
    logTrace(s"Received request: $message")

    message match {
      case openBlocks: OpenBlocks =>
        BlockId(openBlocks.blockIds(0)) match {
          case _: ShuffleBlockId =>
            val blocksNum = openBlocks.blockIds.length
            val contiguousShuffleBlockIds = mergeContiguousShuffleBlocks(openBlocks.blockIds)
            val contBlocksNum = contiguousShuffleBlockIds.length
            val blocks = for (i <- (0 until contBlocksNum).view)
              yield blockManager.getBlockData(contiguousShuffleBlockIds(i))
            val streamId = streamManager.registerStream(
              appId, blocks.flatMap(identity).iterator.asJava)
            logTrace(s"Registered streamId $streamId with $blocksNum buffers")
            responseContext.onSuccess(new StreamHandle(streamId, blocksNum).toByteBuffer)
          case _ =>
            val blocksNum = openBlocks.blockIds.length
            val blocks = for (i <- (0 until blocksNum).view)
              yield blockManager.getBlockData(BlockId.apply(openBlocks.blockIds(i)))
            val streamId = streamManager.registerStream(appId, blocks.iterator.asJava)
            logTrace(s"Registered streamId $streamId with $blocksNum buffers")
            responseContext.onSuccess(new StreamHandle(streamId, blocksNum).toByteBuffer)
        }

      case uploadBlock: UploadBlock =>
        // StorageLevel and ClassTag are serialized as bytes using our JavaSerializer.
        val (level: StorageLevel, classTag: ClassTag[_]) = {
          serializer
            .newInstance()
            .deserialize(ByteBuffer.wrap(uploadBlock.metadata))
            .asInstanceOf[(StorageLevel, ClassTag[_])]
        }
        val data = new NioManagedBuffer(ByteBuffer.wrap(uploadBlock.blockData))
        val blockId = BlockId(uploadBlock.blockId)
        blockManager.putBlockData(blockId, data, level, classTag)
        responseContext.onSuccess(ByteBuffer.allocate(0))
    }
  }

  override def getStreamManager(): StreamManager = streamManager
}

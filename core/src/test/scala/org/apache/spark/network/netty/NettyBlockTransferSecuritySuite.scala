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

import java.io.InputStreamReader
import java.nio._
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

import com.google.common.io.CharStreams
import org.mockito.Mockito._
import org.scalatest.Matchers
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.network.{BlockDataManager, BlockTransferService}
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.BlockFetchingListener
import org.apache.spark.storage.{BlockId, ShuffleBlockId}
import org.apache.spark.util.ThreadUtils

class NettyBlockTransferSecuritySuite extends SparkFunSuite with MockitoSugar with Matchers {
  test("security default off") {
    val conf = new SparkConf()
      .set("spark.app.id", "app-id")
    testConnection(conf, conf) match {
      case Success(_) => // expected
      case Failure(t) => fail(t)
    }
  }

  test("security on same password") {
    val conf = new SparkConf()
      .set("spark.authenticate", "true")
      .set("spark.authenticate.secret", "good")
      .set("spark.app.id", "app-id")
    testConnection(conf, conf) match {
      case Success(_) => // expected
      case Failure(t) => fail(t)
    }
  }

  test("security on mismatch password") {
    val conf0 = new SparkConf()
      .set("spark.authenticate", "true")
      .set("spark.authenticate.secret", "good")
      .set("spark.app.id", "app-id")
    val conf1 = conf0.clone.set("spark.authenticate.secret", "bad")
    testConnection(conf0, conf1) match {
      case Success(_) => fail("Should have failed")
      case Failure(t) => t.getMessage should include ("Mismatched response")
    }
  }

  test("security mismatch auth off on server") {
    val conf0 = new SparkConf()
      .set("spark.authenticate", "true")
      .set("spark.authenticate.secret", "good")
      .set("spark.app.id", "app-id")
    val conf1 = conf0.clone.set("spark.authenticate", "false")
    testConnection(conf0, conf1) match {
      case Success(_) => fail("Should have failed")
      case Failure(t) => // any funny error may occur, sever will interpret SASL token as RPC
    }
  }

  test("security mismatch auth off on client") {
    val conf0 = new SparkConf()
      .set("spark.authenticate", "false")
      .set("spark.authenticate.secret", "good")
      .set("spark.app.id", "app-id")
    val conf1 = conf0.clone.set("spark.authenticate", "true")
    testConnection(conf0, conf1) match {
      case Success(_) => fail("Should have failed")
      case Failure(t) => t.getMessage should include ("Expected SaslMessage")
    }
  }

  test("security with aes encryption") {
    val conf = new SparkConf()
      .set("spark.authenticate", "true")
      .set("spark.authenticate.secret", "good")
      .set("spark.app.id", "app-id")
      .set("spark.network.crypto.enabled", "true")
      .set("spark.network.crypto.saslFallback", "false")
    testConnection(conf, conf) match {
      case Success(_) => // expected
      case Failure(t) => fail(t)
    }
  }


  /**
   * Creates two servers with different configurations and sees if they can talk.
   * Returns Success() if they can transfer a block, and Failure() if the block transfer was failed
   * properly. We will throw an out-of-band exception if something other than that goes wrong.
   */
  private def testConnection(conf0: SparkConf, conf1: SparkConf): Try[Unit] = {
    val blockManager = mock[BlockDataManager]
    val blockId = ShuffleBlockId(0, 1, 2)
    val blockString = "Hello, world!"
    val blockBuffer = new NioManagedBuffer(ByteBuffer.wrap(
      blockString.getBytes(StandardCharsets.UTF_8)))
    when(blockManager.getBlockData(blockId)).thenReturn(blockBuffer)

    val securityManager0 = new SecurityManager(conf0)
    val exec0 = new NettyBlockTransferService(conf0, securityManager0, "localhost", "localhost", 0,
      1)
    exec0.init(blockManager)

    val securityManager1 = new SecurityManager(conf1)
    val exec1 = new NettyBlockTransferService(conf1, securityManager1, "localhost", "localhost", 0,
      1)
    exec1.init(blockManager)

    val result = fetchBlock(exec0, exec1, "1", blockId) match {
      case Success(buf) =>
        val actualString = CharStreams.toString(
          new InputStreamReader(buf.createInputStream(), StandardCharsets.UTF_8))
        actualString should equal(blockString)
        buf.release()
        Success(())
      case Failure(t) =>
        Failure(t)
    }
    exec0.close()
    exec1.close()
    result
  }

  /** Synchronously fetches a single block, acting as the given executor fetching from another. */
  private def fetchBlock(
      self: BlockTransferService,
      from: BlockTransferService,
      execId: String,
      blockId: BlockId): Try[ManagedBuffer] = {

    val promise = Promise[ManagedBuffer]()

    self.fetchBlocks(from.hostName, from.port, execId, Array(blockId.toString),
      new BlockFetchingListener {
        override def onBlockFetchFailure(blockIds: Array[String], exception: Throwable): Unit = {
          promise.failure(exception)
        }

        override def onBlockFetchSuccess(blockIds: Array[String], data: ManagedBuffer): Unit = {
          promise.success(data.retain())
        }
      }, null, false)

    ThreadUtils.awaitReady(promise.future, FiniteDuration(10, TimeUnit.SECONDS))
    promise.future.value.get
  }
}


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

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.benchmark.Benchmark

/**
 * Benchmark to measure performance for wide table.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/WideTableBenchmark-results.txt".
 * }}}
 */
object WideTableBenchmark extends SqlBasedBenchmark {

  override def runBenchmarkSuite(): Unit = {
    runBenchmark("projection on wide table: simple") {
      val N = 1 << 20
      val df = spark.range(N)
      val columns = (0 until 400).map{ i => s"id as id$i"}
      val benchmark = new Benchmark("projection on wide table: simple", N, output = output)
      Seq("10", "100", "1024", "2048", "4096", "8196", "65536").foreach { n =>
        benchmark.addCase(s"split threshold $n", numIters = 5) { iter =>
          withSQLConf("spark.sql.codegen.methodSplitThreshold" -> n) {
            df.selectExpr(columns: _*).foreach(identity(_))
          }
        }
      }
      benchmark.run()
    }

    runBenchmark("projection on wide table: long alias names") {
      val N = 1 << 20
      val df = spark.range(N)
      val longName = "averylongaliasname" * 20
      val columns = (0 until 400).map{ i => s"id as ${longName}_id$i"}
      val benchmark =
        new Benchmark("projection on wide table: long alias names", N, output = output)
      Seq("10", "100", "1024", "2048", "4096", "8196", "65536").foreach { n =>
        benchmark.addCase(s"split threshold $n", numIters = 5) { iter =>
          withSQLConf("spark.sql.codegen.methodSplitThreshold" -> n) {
            df.selectExpr(columns: _*).foreach(identity(_))
          }
        }
      }
      benchmark.run()
    }

    runBenchmark("projection on wide table: complex expressions 1") {
      val N = 1 << 20
      val df = spark.range(N)
      val columns = (0 until 400).map{ i => s"case when id = $i then $i else 800 end as id$i"}
      val benchmark =
        new Benchmark("projection on wide table: complex expressions 1", N, output = output)
      Seq("10", "100", "1024", "2048", "4096", "8196", "65536").foreach { n =>
        benchmark.addCase(s"split threshold $n", numIters = 5) { iter =>
          withSQLConf("spark.sql.codegen.methodSplitThreshold" -> n) {
            df.selectExpr(columns: _*).foreach(identity(_))
          }
        }
      }
      benchmark.run()
    }

    runBenchmark("projection on wide table: complex expressions 2") {
      val N = 1 << 20
      val df = spark.range(N)
      val tmp = (0 until 6).map(i => s"when id = ${N + i}L then 1")
      val expr = s"case ${tmp.mkString(" ")} else sqrt($N) end"
      val columns = (0 until 400).map{ i => s"$expr as id$i" }
      val benchmark =
        new Benchmark("projection on wide table: complex expressions 2", N, output = output)
      Seq("10", "100", "1024", "2048", "4096", "8196", "65536").foreach { n =>
        benchmark.addCase(s"split threshold $n", numIters = 5) { iter =>
          withSQLConf("spark.sql.codegen.methodSplitThreshold" -> n) {
            df.selectExpr(columns: _*).foreach(identity(_))
          }
        }
      }
      benchmark.run()
    }
  }
}

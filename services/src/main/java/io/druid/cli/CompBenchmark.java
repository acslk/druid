/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.cli;

import com.google.common.io.Files;
import io.druid.segment.data.CompressedLongsIndexedSupplier;
import io.druid.segment.data.IndexedLongs;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class CompBenchmark
{
  @Param("/Users/daveli/gitsegment/2016-08-15T15:00:00.000Z_2016-08-15T16:00:00.000Z/2016-08-15T15:00:15.484Z/1/temp/")
  private String directory;

  @Param("commits-LZ4-AUTO")
  private String fileName;

  private Random rand;
  private IndexedLongs indexedLongs;

  @Setup
  public void setup() throws Exception
  {
    File dir = new File(directory);
    File compFile = new File(dir, fileName);
    ByteBuffer buffer = Files.map(compFile);
    indexedLongs = CompressedLongsIndexedSupplier.fromByteBuffer(buffer, ByteOrder.nativeOrder()).get();
    rand = new Random();
  }

  @Benchmark
  public void readContinuous(Blackhole bh) throws Exception
  {
    long sum = 0;
    for (int i = 0; i < indexedLongs.size(); i++) {
      sum += indexedLongs.get(i);
    }
    bh.consume(sum);
  }

  @Benchmark
  public void readSkipping(Blackhole bh) throws Exception
  {
    long sum = 0;
    for (int i = 0; i < indexedLongs.size(); i += rand.nextInt(1000)) {
      sum += indexedLongs.get(i);
    }
    bh.consume(sum);
  }

}

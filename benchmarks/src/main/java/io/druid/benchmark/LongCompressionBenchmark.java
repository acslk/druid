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

package io.druid.benchmark;

import com.google.common.base.Supplier;
import com.google.common.io.Files;
import io.druid.segment.data.BlockLayoutIndexedLongSupplier;
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import java.util.concurrent.TimeUnit;

// Run LongCompressionBenchmarkFileGenerator to generate the required files before running this benchmark

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class LongCompressionBenchmark
{
  @Param({"enumerate"})
  private static String file;

  @Param({"longs"})
  private static String format;

  @Param({"lz4"})
  private static String strategy;

//  @Param({"1", "2", "4", "8", "16", "32", "128", "512"})
//  private static int skip;

  private Random rand;
  private Supplier<IndexedLongs> supplier;
  private long sum;

  @Setup
  public void setup() throws Exception
  {
    URL url = this.getClass().getClassLoader().getResource("compress");
    File dir = new File(url.toURI());
    File compFile = new File(dir, file + "-" + strategy.toUpperCase() + "-" + format.toUpperCase());
    rand = new Random();
    ByteBuffer buffer = Files.map(compFile);
    supplier = CompressedLongsIndexedSupplier.fromByteBuffer(buffer, ByteOrder.nativeOrder());
  }

  @Benchmark
  public void readContinuous(Blackhole bh) throws IOException
  {
    BlockLayoutIndexedLongSupplier.BlockLayoutIndexedLongs indexedLongs = (BlockLayoutIndexedLongSupplier.BlockLayoutIndexedLongs)supplier.get();
    int count = indexedLongs.size();
    sum = 0;
    for (int i = 0; i < count; i++) {
      sum += indexedLongs.get(i);
    }
    System.out.println(indexedLongs.debugEnter);
    bh.consume(sum);
    indexedLongs.close();
  }

  @Benchmark
  public void readSkipping(Blackhole bh) throws IOException
  {
    BlockLayoutIndexedLongSupplier.BlockLayoutIndexedLongs indexedLongs = (BlockLayoutIndexedLongSupplier.BlockLayoutIndexedLongs)supplier.get();
    int count = indexedLongs.size();
    sum = 0;
    for (int i = 0; i < count; i += rand.nextInt(2000)) {
      sum += indexedLongs.get(i);
    }
    System.out.println(indexedLongs.debugEnter);
    bh.consume(sum);
    indexedLongs.close();
  }

}


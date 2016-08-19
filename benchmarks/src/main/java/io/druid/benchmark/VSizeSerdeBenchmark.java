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

import com.google.common.base.Throwables;
import com.google.common.io.Files;
import io.druid.segment.CompressedPools;
import io.druid.segment.data.VSizeLongSerde;
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class VSizeSerdeBenchmark
{
  @Param({"5000000"})
  private int values;

  @Param({"1", "2", "4", "8", "12", "16", "20", "24", "32", "40", "48", "56", "64"})
  private int size;

  private VSizeLongSerde.LongDeserializer mappedDeserializer;
  private VSizeLongSerde.LongDeserializer directDeserializer;
  private static File dummy;

  {
    try {
      // this uses a dummy file of sufficient size to construct a mappedByteBuffer instead of using ByteBuffer.allocate
      // to construct a heapByteBuffer since they have different performance
      dummy = File.createTempFile("vsizeserde", "dummy");
      dummy.deleteOnExit();
      try (Writer writer = new BufferedWriter(new FileWriter(dummy))) {
        for (int i = 0; i < 5000000 * 8 + 20; i++) {
          writer.write(0);
        }
      }
    }
    catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  @Setup
  public void setup() throws IOException, URISyntaxException
  {
    ByteBuffer mappedBuffer = Files.map(dummy);
    ByteBuffer directBuffer = ByteBuffer.allocateDirect(CompressedPools.BUFFER_SIZE);
    mappedDeserializer = VSizeLongSerde.getDeserializer(size, mappedBuffer, 10);
    directDeserializer = VSizeLongSerde.getDeserializer(size, directBuffer, 0);
  }

  @Benchmark
  public void readMapped(Blackhole bh)
  {
    long sum = 0;
    for (int i = 0; i < values; i++) {
      sum += mappedDeserializer.get(i);
    }
    bh.consume(sum);
  }

  @Benchmark
  public void readDirect(Blackhole bh)
  {
    long sum = 0;
    int blockSize = VSizeLongSerde.getBlockSize(size, CompressedPools.BUFFER_SIZE);
    int blocks = values / blockSize;
    int remaining = values % blockSize;
    for (int i = 0; i < blocks; i++) {
      for (int j = 0; j < blockSize; j++) {
        sum += directDeserializer.get(j);
      }
    }
    for (int i = 0; i < remaining; i++) {
      sum += directDeserializer.get(i);
    }
    bh.consume(sum);
  }
}

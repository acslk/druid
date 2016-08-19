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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

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
public class ByteBufferBenchmark
{
  private ByteBuffer mappedBuffer;
  private ByteBuffer directBuffer;
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
    mappedBuffer = Files.map(dummy);
    directBuffer = ByteBuffer.allocateDirect(CompressedPools.BUFFER_SIZE);
  }

  @Benchmark
  public void copy() {
    for (int i = 0; i < 5000000/8192 + 1; i++) {
      final ByteBuffer copyBuffer = mappedBuffer.duplicate();
      copyBuffer.limit(copyBuffer.position() + CompressedPools.BUFFER_SIZE);
      directBuffer.put(copyBuffer).flip();
      mappedBuffer.position(mappedBuffer.position() + 1);
    }
  }

}

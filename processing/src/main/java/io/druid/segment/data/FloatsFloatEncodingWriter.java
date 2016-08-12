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

package io.druid.segment.data;

import com.google.common.primitives.Floats;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class FloatsFloatEncodingWriter implements CompressionFactory.FloatEncodingWriter
{
  private final ByteBuffer orderBuffer;
  private final ByteOrder order;
  private ByteBuffer outBuffer = null;
  private OutputStream outStream = null;

  public FloatsFloatEncodingWriter(ByteOrder order)
  {
    this.order = order;
    orderBuffer = ByteBuffer.allocate(Floats.BYTES);
    orderBuffer.order(order);
  }

  @Override
  public void setBuffer(ByteBuffer buffer)
  {
    outStream = null;
    outBuffer = buffer;
    // this order change is safe as the buffer is passed in and allocated in BlockLayoutFloatSupplierSerializer, and
    // is used only as a temporary storage to be written
    outBuffer.order(order);
  }

  @Override
  public void setOutputStream(OutputStream output)
  {
    outBuffer = null;
    outStream = output;
  }

  @Override
  public void write(float value) throws IOException
  {
    if (outBuffer != null) {
      outBuffer.putFloat(value);
    }
    if (outStream != null) {
      orderBuffer.rewind();
      orderBuffer.putFloat(value);
      outStream.write(orderBuffer.array());
    }
  }

  @Override
  public void flush() throws IOException
  {
  }

  @Override
  public void putMeta(OutputStream metaOut, CompressedObjectStrategy.CompressionStrategy strategy) throws IOException
  {
    metaOut.write(strategy.getId());
  }

  @Override
  public int getBlockSize(int bytesPerBlock)
  {
    return bytesPerBlock / Floats.BYTES;
  }

  @Override
  public int getNumBytes(int values)
  {
    return values * Floats.BYTES;
  }
}

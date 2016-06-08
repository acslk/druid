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

package io.druid.segment;

import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.LongSupplierSerializer;
import io.druid.segment.data.IOPeon;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class LongColumnSerializer implements GenericColumnSerializer
{
  public static LongColumnSerializer create(
      IOPeon ioPeon,
      String filenameBase,
      CompressionFactory.CompressionFormat compression
  )
  {
    return new LongColumnSerializer(ioPeon, filenameBase, IndexIO.BYTE_ORDER, compression);
  }

  private final IOPeon ioPeon;
  private final String filenameBase;
  private final ByteOrder byteOrder;
  private final CompressionFactory.CompressionFormat compression;
  private LongSupplierSerializer writer;

  public LongColumnSerializer(
      IOPeon ioPeon,
      String filenameBase,
      ByteOrder byteOrder,
      CompressionFactory.CompressionFormat compression
  )
  {
    this.ioPeon = ioPeon;
    this.filenameBase = filenameBase;
    this.byteOrder = byteOrder;
    this.compression = compression;
  }

  @Override
  public void open() throws IOException
  {
    writer = compression.getLongSerializer(
        ioPeon,
        String.format("%s.long_column", filenameBase),
        byteOrder
    );
    writer.open();
  }

  @Override
  public void serialize(Object obj) throws IOException
  {
    long val = (obj == null) ? 0 : ((Number) obj).longValue();
    writer.add(val);
  }

  @Override
  public void close() throws IOException
  {
    writer.close();
  }

  @Override
  public long getSerializedSize()
  {
    return writer.getSerializedSize();
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    writer.writeToChannel(channel);
  }

}

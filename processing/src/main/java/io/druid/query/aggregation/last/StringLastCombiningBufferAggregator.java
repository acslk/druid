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

package io.druid.query.aggregation.last;

import com.google.common.primitives.Longs;
import io.druid.collections.SerializablePair;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.StringHelper;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;

public class StringLastCombiningBufferAggregator implements BufferAggregator
{
  private final ObjectColumnSelector selector;
  private final int maxLength;

  public StringLastCombiningBufferAggregator(ObjectColumnSelector selector, int maxLength)
  {
    this.selector = selector;
    this.maxLength = maxLength;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, Long.MIN_VALUE);
    buf.putChar(position + Longs.BYTES, '\0');
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    SerializablePair<Long, String> pair = (SerializablePair<Long, String>) selector.get();
    long lastTime = buf.getLong(position);
    if (pair.lhs >= lastTime) {
      buf.putLong(position, pair.lhs);
      StringHelper.putString(buf, position + Longs.BYTES, pair.rhs, maxLength);
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return new SerializablePair<>(
        buf.getLong(position),
        StringHelper.getString(buf, position + Longs.BYTES, maxLength)
    );
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close()
  {

  }
}

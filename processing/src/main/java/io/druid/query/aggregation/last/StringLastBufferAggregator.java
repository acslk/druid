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
import io.druid.segment.DimensionSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.data.IndexedInts;

import java.nio.ByteBuffer;

public class StringLastBufferAggregator implements BufferAggregator
{
  private final LongColumnSelector timeSelector;
  private final DimensionSelector valueSelector;

  public StringLastBufferAggregator(LongColumnSelector timeSelector, DimensionSelector valueSelector)
  {
    this.timeSelector = timeSelector;
    this.valueSelector = valueSelector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, Long.MIN_VALUE);
    buf.putInt(position + Longs.BYTES, Integer.MAX_VALUE);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    long time = timeSelector.get();
    long lastTime = buf.getLong(position);
    if (time >= lastTime) {
      buf.putLong(position, time);
      IndexedInts row = valueSelector.getRow();
      buf.putInt(position + Longs.BYTES, row.size() == 0 ? Integer.MAX_VALUE : row.get(0));
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    int val = buf.getInt(position + Longs.BYTES);
    return new SerializablePair<>(buf.getLong(position), val == Integer.MAX_VALUE ? "" : valueSelector.lookupName(val));
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

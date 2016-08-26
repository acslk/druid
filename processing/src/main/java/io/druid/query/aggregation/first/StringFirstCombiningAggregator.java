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

package io.druid.query.aggregation.first;

import io.druid.collections.SerializablePair;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.DimensionSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.data.IndexedInts;

public class StringFirstCombiningAggregator implements Aggregator
{

  private final ObjectColumnSelector selector;
  private final String name;

  long firstTime;
  String firstValue;

  public StringFirstCombiningAggregator(String name, ObjectColumnSelector selector)
  {
    this.name = name;
    this.selector = selector;

    reset();
  }

  @Override
  public void aggregate()
  {
    SerializablePair<Long, String> pair = (SerializablePair<Long, String>) selector.get();
    if (pair.lhs < firstTime) {
      firstTime = pair.lhs;
      firstValue = pair.rhs;
    }
  }

  @Override
  public void reset()
  {
    firstTime = Long.MAX_VALUE;
    firstValue = "";
  }

  @Override
  public Object get()
  {
    return new SerializablePair<>(firstTime, firstValue);
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public void close()
  {

  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException();
  }
}

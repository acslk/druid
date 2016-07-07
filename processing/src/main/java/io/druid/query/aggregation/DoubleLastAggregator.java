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

package io.druid.query.aggregation;

import io.druid.collections.SerializablePair;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;

public class DoubleLastAggregator implements Aggregator
{

  private final FloatColumnSelector valueSelector;
  private final LongColumnSelector timeSelector;
  private final String name;

  long lastTime;
  double lastValue;

  public DoubleLastAggregator(String name, FloatColumnSelector valueSelector, LongColumnSelector timeSelector)
  {
    this.name = name;
    this.valueSelector = valueSelector;
    this.timeSelector = timeSelector;

    reset();
  }

  @Override
  public void aggregate()
  {
    long time = timeSelector.get();
    if (time >= lastTime) {
      lastTime = timeSelector.get();
      lastValue = valueSelector.get();
    }
  }

  @Override
  public void reset()
  {
    lastTime = Long.MIN_VALUE;
    lastValue = 0;
  }

  @Override
  public Object get()
  {
    return new SerializablePair<>(lastTime, lastValue);
  }

  @Override
  public float getFloat()
  {
    return (float) lastValue;
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
    return (long) lastValue;
  }
}
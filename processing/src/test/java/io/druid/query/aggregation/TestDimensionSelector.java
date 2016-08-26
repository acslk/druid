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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TestDimensionSelector implements DimensionSelector
{
  private final List<Integer[]> column;
  private final Map<String, Integer> ids;
  private final Map<Integer, String> lookup;

  private int pos = 0;

  public TestDimensionSelector(Iterable<String[]> values)
  {
    this.lookup = Maps.newHashMap();
    this.ids = Maps.newHashMap();

    int index = 0;
    for (String[] multiValue : values) {
      for (String value : multiValue) {
        if (!ids.containsKey(value)) {
          ids.put(value, index);
          lookup.put(index, value);
          index++;
        }
      }
    }

    this.column = Lists.newArrayList(
        Iterables.transform(
            values, new Function<String[], Integer[]>()
            {
              @Nullable
              @Override
              public Integer[] apply(@Nullable String[] input)
              {
                return Iterators.toArray(
                    Iterators.transform(
                        Iterators.forArray(input), new Function<String, Integer>()
                        {
                          @Nullable
                          @Override
                          public Integer apply(@Nullable String input)
                          {
                            return ids.get(input);
                          }
                        }
                    ), Integer.class
                );
              }
            }
        )
    );
  }

  public void increment()
  {
    pos++;
  }

  public void reset()
  {
    pos = 0;
  }

  @Override
  public IndexedInts getRow()
  {
    final int p = this.pos;
    return new IndexedInts()
    {
      @Override
      public int size()
      {
        return column.get(p).length;
      }

      @Override
      public int get(int i)
      {
        return column.get(p)[i];
      }

      @Override
      public Iterator<Integer> iterator()
      {
        return Iterators.forArray(column.get(p));
      }

      @Override
      public void fill(int index, int[] toFill)
      {
        throw new UnsupportedOperationException("fill not supported");
      }

      @Override
      public void close() throws IOException
      {

      }
    };
  }

  @Override
  public int getValueCardinality()
  {
    return 1;
  }

  @Override
  public String lookupName(int i)
  {
    return lookup.get(i);
  }

  @Override
  public int lookupId(String s)
  {
    return ids.get(s);
  }

  public static List<String[]> dimensionValues(Object... values)
  {
    return Lists.transform(
        Lists.newArrayList(values), new Function<Object, String[]>()
        {
          @Nullable
          @Override
          public String[] apply(@Nullable Object input)
          {
            if (input instanceof String[]) {
              return (String[]) input;
            } else {
              return new String[]{(String) input};
            }
          }
        }
    );
  }
}
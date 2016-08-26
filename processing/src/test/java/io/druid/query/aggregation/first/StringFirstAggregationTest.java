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

import com.google.common.collect.ImmutableList;
import com.metamx.common.Pair;
import io.druid.collections.SerializablePair;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.TestDimensionSelector;
import io.druid.query.aggregation.TestLongColumnSelector;
import io.druid.query.aggregation.TestObjectColumnSelector;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.Column;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class StringFirstAggregationTest
{
  private StringFirstAggregatorFactory aggFactory;
  private StringFirstAggregatorFactory combiningAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestLongColumnSelector timeSelector;
  private TestDimensionSelector valueSelector;
  private TestObjectColumnSelector objectSelector;

  private List<String[]> stringValues = TestDimensionSelector.dimensionValues("a", "bbbb", "c", "last");
  private long[] times = {12, 10, 5344, 7899999};
  private SerializablePair[] pairs = {
      new SerializablePair<>(1467225096L, "aaa"),
      new SerializablePair<>(23163L, "second"),
      new SerializablePair<>(742L, "firstv"),
      new SerializablePair<>(111111L, "other")
  };

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder()
  {
    return ImmutableList.of(new Object[]{new StringFirstAggregatorFactory("billy", "nilly", null)},
                            new Object[]{new StringFirstAggregatorFactory("billy", "nilly", 2)});
  }

  public StringFirstAggregationTest (StringFirstAggregatorFactory aggFactory) {
    this.aggFactory = aggFactory;
    combiningAggFactory = (StringFirstAggregatorFactory) aggFactory.getCombiningFactory();
    timeSelector = new TestLongColumnSelector(times);
    valueSelector = new TestDimensionSelector(stringValues);
    objectSelector = new TestObjectColumnSelector(pairs);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeLongColumnSelector(Column.TIME_COLUMN_NAME)).andReturn(timeSelector);
    EasyMock.expect(colSelectorFactory.makeDimensionSelector(new DefaultDimensionSpec("nilly", "nilly")))
            .andReturn(valueSelector);
    EasyMock.expect(colSelectorFactory.makeObjectColumnSelector("billy")).andReturn(objectSelector);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testStringFirstAggregator()
  {
    Aggregator agg = aggFactory.factorize(colSelectorFactory);

    Assert.assertEquals("billy", agg.getName());

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, String> result = (Pair<Long, String>) agg.get();

    Assert.assertEquals(times[1], result.lhs.longValue());
    Assert.assertEquals(stringValues.get(1)[0], result.rhs);

    agg.reset();
    Assert.assertEquals("", ((Pair<Long, String>) agg.get()).rhs);
  }

  @Test
  public void testStringFirstBufferAggregator()
  {
    BufferAggregator agg = aggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[aggFactory.getMaxIntermediateSize()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Pair<Long, String> result = (Pair<Long, String>) agg.get(buffer, 0);

    Assert.assertEquals(times[1], result.lhs.longValue());
    Assert.assertEquals(stringValues.get(1)[0], result.rhs);
  }

  @Test
  public void testCombine()
  {
    SerializablePair pair1 = new SerializablePair<>(1467225000L, "firstCombine");
    SerializablePair pair2 = new SerializablePair<>(1467240000L, "secondCombine");
    Assert.assertEquals(pair1, aggFactory.combine(pair1, pair2));
  }

  @Test
  public void testStringFirstCombiningAggregator()
  {
    Aggregator agg = combiningAggFactory.factorize(colSelectorFactory);

    Assert.assertEquals("billy", agg.getName());

    aggregate(agg);
    aggregate(agg);
    aggregate(agg);
    aggregate(agg);

    Pair<Long, String> result = (Pair<Long, String>) agg.get();
    Pair<Long, String> expected = (Pair<Long, String>) pairs[2];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.rhs, result.rhs);

    agg.reset();
    Assert.assertEquals("", ((Pair<Long, String>) agg.get()).rhs);
  }

  @Test
  public void testStringFirstCombiningBufferAggregator()
  {
    BufferAggregator agg = combiningAggFactory.factorizeBuffered(
        colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[combiningAggFactory.getMaxIntermediateSize()]);
    agg.init(buffer, 0);

    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);
    aggregate(agg, buffer, 0);

    Pair<Long, String> result = (Pair<Long, String>) agg.get(buffer, 0);
    Pair<Long, String> expected = (Pair<Long, String>) pairs[2];

    Assert.assertEquals(expected.lhs, result.lhs);
    Assert.assertEquals(expected.rhs.substring(0, Math.min(expected.rhs.length(), combiningAggFactory.getMaxLength())), result.rhs);
  }


  @Test
  public void testSerde() throws Exception
  {
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    String lengthSpec = "";
    if (aggFactory.getMaxLength() != StringFirstAggregatorFactory.DEFAULT_STRLEN) {
      lengthSpec = ",\"maxLength\":" + aggFactory.getMaxLength();
    }
    String stringSpecJson = "{\"type\":\"stringFirst\",\"name\":\"billy\",\"fieldName\":\"nilly\"" + lengthSpec + "}";
    Assert.assertEquals(aggFactory, mapper.readValue(stringSpecJson, AggregatorFactory.class));
  }

  private void aggregate(
      Aggregator agg
  )
  {
    agg.aggregate();
    timeSelector.increment();
    valueSelector.increment();
    objectSelector.increment();
  }

  private void aggregate(
      BufferAggregator agg,
      ByteBuffer buff,
      int position
  )
  {
    agg.aggregate(buff, position);
    timeSelector.increment();
    valueSelector.increment();
    objectSelector.increment();
  }
}

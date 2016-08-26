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

package io.druid.query.aggregation.cardinality;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.TestDimensionSelector;
import io.druid.segment.DimensionSelector;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class CardinalityAggregatorTest
{


  /*
    values1: 4 distinct rows
    values1: 4 distinct values
    values2: 8 distinct rows
    values2: 7 distinct values
    groupBy(values1, values2): 9 distinct rows
    groupBy(values1, values2): 7 distinct values
    combine(values1, values2): 8 distinct rows
    combine(values1, values2): 7 distinct values
   */
  private static final List<String[]> values1 = TestDimensionSelector.dimensionValues(
      "a", "b", "c", "a", "a", null, "b", "b", "b", "b", "a", "a"
  );
  private static final List<String[]> values2 = TestDimensionSelector.dimensionValues(
      "a",
      "b",
      "c",
      "x",
      "a",
      "e",
      "b",
      new String[]{null, "x"},
      new String[]{"x", null},
      new String[]{"y", "x"},
      new String[]{"x", "y"},
      new String[]{"x", "y", "a"}
  );

  private static void aggregate(List<DimensionSelector> selectorList, Aggregator agg)
  {
    agg.aggregate();

    for (DimensionSelector selector : selectorList) {
      ((TestDimensionSelector) selector).increment();
    }
  }

  private static void bufferAggregate(
      List<DimensionSelector> selectorList,
      BufferAggregator agg,
      ByteBuffer buf,
      int pos
  )
  {
    agg.aggregate(buf, pos);

    for (DimensionSelector selector : selectorList) {
      ((TestDimensionSelector) selector).increment();
    }
  }

  List<DimensionSelector> selectorList;
  CardinalityAggregatorFactory rowAggregatorFactory;
  CardinalityAggregatorFactory valueAggregatorFactory;
  final TestDimensionSelector dim1;
  final TestDimensionSelector dim2;

  public CardinalityAggregatorTest()
  {
    dim1 = new TestDimensionSelector(values1);
    dim2 = new TestDimensionSelector(values2);

    selectorList = Lists.newArrayList(
        (DimensionSelector) dim1,
        dim2
    );

    rowAggregatorFactory = new CardinalityAggregatorFactory(
        "billy",
        Lists.newArrayList("dim1", "dim2"),
        true
    );

    valueAggregatorFactory = new CardinalityAggregatorFactory(
        "billy",
        Lists.newArrayList("dim1", "dim2"),
        true
    );
  }

  @Test
  public void testAggregateRows() throws Exception
  {
    CardinalityAggregator agg = new CardinalityAggregator(
        "billy",
        selectorList,
        true
    );


    for (int i = 0; i < values1.size(); ++i) {
      aggregate(selectorList, agg);
    }
    Assert.assertEquals(9.0, (Double) rowAggregatorFactory.finalizeComputation(agg.get()), 0.05);
  }

  @Test
  public void testAggregateValues() throws Exception
  {
    CardinalityAggregator agg = new CardinalityAggregator(
        "billy",
        selectorList,
        false
    );

    for (int i = 0; i < values1.size(); ++i) {
      aggregate(selectorList, agg);
    }
    Assert.assertEquals(7.0, (Double) valueAggregatorFactory.finalizeComputation(agg.get()), 0.05);
  }

  @Test
  public void testBufferAggregateRows() throws Exception
  {
    CardinalityBufferAggregator agg = new CardinalityBufferAggregator(
        selectorList,
        true
    );

    int maxSize = rowAggregatorFactory.getMaxIntermediateSize();
    ByteBuffer buf = ByteBuffer.allocate(maxSize + 64);
    int pos = 10;
    buf.limit(pos + maxSize);

    agg.init(buf, pos);

    for (int i = 0; i < values1.size(); ++i) {
      bufferAggregate(selectorList, agg, buf, pos);
    }
    Assert.assertEquals(9.0, (Double) rowAggregatorFactory.finalizeComputation(agg.get(buf, pos)), 0.05);
  }

  @Test
  public void testBufferAggregateValues() throws Exception
  {
    CardinalityBufferAggregator agg = new CardinalityBufferAggregator(
        selectorList,
        false
    );

    int maxSize = valueAggregatorFactory.getMaxIntermediateSize();
    ByteBuffer buf = ByteBuffer.allocate(maxSize + 64);
    int pos = 10;
    buf.limit(pos + maxSize);

    agg.init(buf, pos);

    for (int i = 0; i < values1.size(); ++i) {
      bufferAggregate(selectorList, agg, buf, pos);
    }
    Assert.assertEquals(7.0, (Double) valueAggregatorFactory.finalizeComputation(agg.get(buf, pos)), 0.05);
  }

  @Test
  public void testCombineRows()
  {
    List<DimensionSelector> selector1 = Lists.newArrayList((DimensionSelector) dim1);
    List<DimensionSelector> selector2 = Lists.newArrayList((DimensionSelector) dim2);

    CardinalityAggregator agg1 = new CardinalityAggregator("billy", selector1, true);
    CardinalityAggregator agg2 = new CardinalityAggregator("billy", selector2, true);

    for (int i = 0; i < values1.size(); ++i) {
      aggregate(selector1, agg1);
    }
    for (int i = 0; i < values2.size(); ++i) {
      aggregate(selector2, agg2);
    }

    Assert.assertEquals(4.0, (Double) rowAggregatorFactory.finalizeComputation(agg1.get()), 0.05);
    Assert.assertEquals(8.0, (Double) rowAggregatorFactory.finalizeComputation(agg2.get()), 0.05);

    Assert.assertEquals(
        9.0,
        (Double) rowAggregatorFactory.finalizeComputation(
            rowAggregatorFactory.combine(
                agg1.get(),
                agg2.get()
            )
        ),
        0.05
    );
  }

  @Test
  public void testCombineValues()
  {
    List<DimensionSelector> selector1 = Lists.newArrayList((DimensionSelector) dim1);
    List<DimensionSelector> selector2 = Lists.newArrayList((DimensionSelector) dim2);

    CardinalityAggregator agg1 = new CardinalityAggregator("billy", selector1, false);
    CardinalityAggregator agg2 = new CardinalityAggregator("billy", selector2, false);

    for (int i = 0; i < values1.size(); ++i) {
      aggregate(selector1, agg1);
    }
    for (int i = 0; i < values2.size(); ++i) {
      aggregate(selector2, agg2);
    }

    Assert.assertEquals(4.0, (Double) valueAggregatorFactory.finalizeComputation(agg1.get()), 0.05);
    Assert.assertEquals(7.0, (Double) valueAggregatorFactory.finalizeComputation(agg2.get()), 0.05);

    Assert.assertEquals(
        7.0,
        (Double) rowAggregatorFactory.finalizeComputation(
            rowAggregatorFactory.combine(
                agg1.get(),
                agg2.get()
            )
        ),
        0.05
    );
  }

  @Test
  public void testSerde() throws Exception
  {
    CardinalityAggregatorFactory factory = new CardinalityAggregatorFactory(
        "billy",
        ImmutableList.of("b", "a", "c"),
        true
    );
    ObjectMapper objectMapper = new DefaultObjectMapper();
    Assert.assertEquals(
        factory,
        objectMapper.readValue(objectMapper.writeValueAsString(factory), AggregatorFactory.class)
    );
  }
}

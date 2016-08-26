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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Chars;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.common.StringUtils;
import io.druid.collections.SerializablePair;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.first.DoubleFirstAggregatorFactory;
import io.druid.query.aggregation.first.StringFirstAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.Column;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class StringLastAggregatorFactory extends AggregatorFactory
{
  public static final int MAX_STRLEN = 1000;
  public static final int DEFAULT_STRLEN = 100;

  private static final byte CACHE_TYPE_ID = 24;

  private final String fieldName;
  private final String name;
  private final int maxLength;

  @JsonCreator
  public StringLastAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("maxLength") final Integer maxLength
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.name = name;
    this.fieldName = fieldName;
    this.maxLength = maxLength == null ? DEFAULT_STRLEN : Math.min(maxLength, MAX_STRLEN);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory columnFactory)
  {
    return new StringLastAggregator(
        name,
        columnFactory.makeLongColumnSelector(Column.TIME_COLUMN_NAME),
        columnFactory.makeDimensionSelector(new DefaultDimensionSpec(fieldName, fieldName))
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory)
  {
    return new StringLastBufferAggregator(
        columnFactory.makeLongColumnSelector(Column.TIME_COLUMN_NAME),
        columnFactory.makeDimensionSelector(new DefaultDimensionSpec(fieldName, fieldName))
    );
  }

  @Override
  public Comparator getComparator()
  {
    return StringFirstAggregatorFactory.VALUE_COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return DoubleFirstAggregatorFactory.TIME_COMPARATOR.compare(lhs, rhs) > 0 ? lhs : rhs;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new StringLastAggregatorFactory(name, name, maxLength)
    {
      @Override
      public Aggregator factorize(ColumnSelectorFactory columnFactory)
      {
        return new StringLastCombiningAggregator(
            name,
            columnFactory.makeObjectColumnSelector(name)
        );
      }

      @Override
      public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory)
      {
        return new StringLastCombiningBufferAggregator(
            columnFactory.makeObjectColumnSelector(name),
            maxLength
        );
      }

      @Override
      public int getMaxIntermediateSize()
      {
        return Longs.BYTES + maxLength * Chars.BYTES;
      }
    };
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && this.getClass() == other.getClass()) {
      return getCombiningFactory();
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new StringLastAggregatorFactory(fieldName, fieldName, maxLength));
  }

  @Override
  public Object deserialize(Object object)
  {
    Map map = (Map) object;
    return new SerializablePair<>(((Number) map.get("lhs")).longValue(), (String)map.get("rhs"));
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((SerializablePair<Long, String>) object).rhs;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public int getMaxLength() {
    return maxLength;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(Column.TIME_COLUMN_NAME, fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);

    return ByteBuffer.allocate(1 + fieldNameBytes.length).put(CACHE_TYPE_ID).put(fieldNameBytes).array();
  }

  @Override
  public String getTypeName()
  {
    return "string";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Longs.BYTES + Ints.BYTES;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StringLastAggregatorFactory that = (StringLastAggregatorFactory) o;

    return fieldName.equals(that.fieldName) && name.equals(that.name) && maxLength == that.maxLength;
  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + fieldName.hashCode();
    result = 31 * result + maxLength;
    return result;
  }

  @Override
  public String toString()
  {
    return "StringLastAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", maxLength='" + maxLength + '\'' +
           '}';
  }
}

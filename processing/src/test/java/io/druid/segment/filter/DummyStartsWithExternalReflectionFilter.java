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

package io.druid.segment.filter;

import com.google.common.base.Predicate;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.segment.ColumnSelectorFactory;

import java.util.List;


public class DummyStartsWithExternalReflectionFilter extends ExternalReflectionFilter
{


  private final class StartsWithDimensionPredicateFilter extends DimensionPredicateFilter
  {
    public StartsWithDimensionPredicateFilter(
        final String dimension,
        final String prefix
    )
    {
      super(
          dimension,
          new Predicate<String>()
          {
            @Override
            public boolean apply(String input)
            {
              return (input != null) && (prefix != null) && input.startsWith(prefix);
            }
          }
      );
    }
  }

  private final StartsWithDimensionPredicateFilter filter;


  public DummyStartsWithExternalReflectionFilter(List<String> constructorParams)
  {
    super(constructorParams);

    filter = new StartsWithDimensionPredicateFilter(
        constructorParams.get(0),
        constructorParams.get(1)
    );
  }


  @Override
  public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector)
  {
    return filter.getBitmapIndex(selector);
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    return filter.makeMatcher(factory);
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
  {
    return filter.makeMatcher(columnSelectorFactory);
  }
}

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

package io.druid.query.extraction;

import java.util.List;

public class DummySubstringExternalReflectionExtraction extends ExternalReflectionExtraction
{


  private int beginIndex;
  private int endIndex;

  public DummySubstringExternalReflectionExtraction(List<String> constructorParams)
  {
    super(constructorParams);

    // may throw
    beginIndex = Integer.valueOf(constructorParams.get(0));
    endIndex = Integer.valueOf(constructorParams.get(1));

  }

  @Override
  public String apply(Object value)
  {
    return apply(value == null ? null : value.toString());
  }

  @Override
  public String apply(long value)
  {
    return apply(Long.toString(value));
  }


  @Override
  public String apply(String value)
  {
    return value == null ? null : value.substring(beginIndex, endIndex);
  }


  @Override
  public boolean preservesOrdering()
  {
    return false;
  }

  @Override
  public ExtractionFn.ExtractionType getExtractionType()
  {
    return ExtractionFn.ExtractionType.ONE_TO_ONE;
  }
}

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

public abstract class ExternalReflectionExtraction
{
  protected List<String> constructorParams;

  public ExternalReflectionExtraction(List<String> constructorParams)
  {
    this.constructorParams = constructorParams;
  }

  public static ExternalReflectionExtraction createReflectionFilterBase(
      String className,
      List<String> constructorParams
  ) throws Exception
  {
    Class<?> clazz = Class.forName(className);

    if (!ExternalReflectionExtraction.class.isAssignableFrom(clazz)) {
      throw new IllegalArgumentException(
          String.format(
              "The class '%s' is not assignable to '%s'.",
              className,
              ExternalReflectionExtraction.class.getName()
          )
      );
    }
    return (ExternalReflectionExtraction) clazz.getConstructor(List.class).newInstance(constructorParams);
  }

  abstract public String apply(Object value);

  abstract public String apply(String value);

  abstract public String apply(long value);

  abstract public boolean preservesOrdering();

  abstract public ExtractionFn.ExtractionType getExtractionType();
}

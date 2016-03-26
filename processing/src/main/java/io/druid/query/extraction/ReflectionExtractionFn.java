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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.metamx.common.StringUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ReflectionExtractionFn implements ExtractionFn
{

  private final String className;
  private final List<String> constructorParams;
  private final ExternalReflectionExtraction underlineExtraction;

  @JsonCreator
  public ReflectionExtractionFn(
      @JsonProperty("className") String className,
      @JsonProperty("constructorParams") List<String> constructorParams
  )
  {
    Preconditions.checkArgument(className != null, "className must not be null");

    this.className = className;
    this.constructorParams = (constructorParams == null) ? Collections.<String>emptyList() : constructorParams;
    try {
      underlineExtraction = ExternalReflectionExtraction.createReflectionFilterBase(
          this.className,
          this.constructorParams
      );
    }
    catch (Exception e) {
      // convert to runtime
      throw new RuntimeException(e);
    }
  }

  @JsonProperty
  public String getClassName()
  {
    return className;
  }

  @JsonProperty
  public List<String> getConstructorParams()
  {
    return constructorParams;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] classNameBytes = StringUtils.toUtf8(className);
    final byte[][] constructorParamsBytes = new byte[constructorParams.size()][];
    int valuesBytesSize = 0;
    int index = 0;
    for (String value : constructorParams) {
      constructorParamsBytes[index] = StringUtils.toUtf8(value);
      valuesBytesSize += constructorParamsBytes[index].length + 1;
      ++index;
    }

    ByteBuffer filterCacheKey = ByteBuffer.allocate(2 + classNameBytes.length + valuesBytesSize)
                                          .put(ExtractionCacheHelper.CACHE_TYPE_ID_REFLECTION)
                                          .put(classNameBytes)
                                          .put((byte) 0xFF);
    for (byte[] bytes : constructorParamsBytes) {
      filterCacheKey.put(bytes)
                    .put((byte) 0xFF);
    }
    return filterCacheKey.array();
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

    ReflectionExtractionFn that = (ReflectionExtractionFn) o;

    if (className != null ? !className.equals(that.className) : that.className != null) {
      return false;
    }
    return constructorParams != null
           ? constructorParams.equals(that.constructorParams)
           : that.constructorParams == null;

  }

  @Override
  public int hashCode()
  {
    int result = className != null ? className.hashCode() : 0;
    result = 31 * result + (constructorParams != null ? constructorParams.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "ReflectionExtractionFn{" +
           "className='" + className + '\'' +
           ", constructorParams=" + Arrays.toString(constructorParams.toArray()) +
           '}';
  }

  @Override
  public String apply(Object value)
  {
    return underlineExtraction.apply(value);
  }

  @Override
  public String apply(String value)
  {
    return underlineExtraction.apply(value);
  }

  @Override
  public String apply(long value)
  {
    return underlineExtraction.apply(value);
  }

  @Override
  public boolean preservesOrdering()
  {
    return underlineExtraction.preservesOrdering();
  }

  @Override
  public ExtractionType getExtractionType()
  {
    return underlineExtraction.getExtractionType();
  }

}

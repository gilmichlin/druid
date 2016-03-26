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

package io.druid.query.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.annotations.Json;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class ReflectionDimFilterSerDesrTest
{
  private static ObjectMapper mapper;

  private final String actualReflectionFilter = "{\"type\":\"reflection\",\"className\":\"com.test.bla\",\"constructorParams\":[\"a\",\"b\"]}";

  @Before
  public void setUp()
  {
    Injector defaultInjector = GuiceInjectors.makeStartupInjector();
    mapper = defaultInjector.getInstance(Key.get(ObjectMapper.class, Json.class));
  }

  @Test
  public void testDeserialization() throws IOException
  {
    final ReflectionDimFilter actualReflectionDimFilter = mapper.reader(ReflectionDimFilter.class)
                                                                .readValue(actualReflectionFilter);
    final ReflectionDimFilter expectedReflectionDimFilter = new ReflectionDimFilter(
        "com.test.bla",
        Arrays.asList("a", "b")
    );
    Assert.assertEquals(expectedReflectionDimFilter, actualReflectionDimFilter);
  }

  @Test
  public void testSerialization() throws IOException
  {
    final ReflectionDimFilter reflectionInFilter = new ReflectionDimFilter("com.test.bla", Arrays.asList("a", "b"));
    final String expectedReflectionFilter = mapper.writeValueAsString(reflectionInFilter);
    Assert.assertEquals(expectedReflectionFilter, actualReflectionFilter);
  }

  @Test
  public void testGetCacheKey()
  {
    final ReflectionDimFilter reflectionDimFilter_1 = new ReflectionDimFilter("com.test.bla", Arrays.asList("a", "b"));
    final ReflectionDimFilter reflectionDimFilter_2 = new ReflectionDimFilter("com.test.bla", Arrays.asList("c", "d"));
    Assert.assertNotEquals(reflectionDimFilter_1.getCacheKey(), reflectionDimFilter_2.getCacheKey());
  }
}

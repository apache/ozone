/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.tracing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Test for {@link TraceAllMethod}.
 */
public class TestTraceAllMethod {

  @Test
  public void testUnknownMethod() {
    TraceAllMethod<Service> subject = new TraceAllMethod<>(new ServiceImpl(),
        "service");
    assertThrows(NoSuchMethodException.class, () -> subject.invoke(null,
        ServiceImpl.class.getMethod("no such method"), new Object[]{}));
  }

  @Test
  public void testDefaultMethod() throws Throwable {
    TraceAllMethod<Service> subject = new TraceAllMethod<>(new ServiceImpl(),
        "service");

    assertEquals("Hello default", subject.invoke(null,
        ServiceImpl.class.getMethod("defaultMethod"), new Object[]{}));
  }

  /**
   * Example interface for testing {@link TraceAllMethod}.
   */
  public interface Service {
    default String defaultMethod() {
      return otherMethod("default");
    }

    String otherMethod(String name);
  }

  /**
   * Example implementation class for testing {@link TraceAllMethod}.
   */
  public static class ServiceImpl implements Service {

    @Override
    public String otherMethod(String name) {
      return "Hello " + name;
    }
  }
}

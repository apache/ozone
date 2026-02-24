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

package org.apache.ozone.test;

import java.lang.reflect.Method;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/**
 * Base class for Ozone JUnit tests.
 * Provides test method name, which can be used to create unique items.
 */
public abstract class OzoneTestBase {

  private static final AtomicInteger OBJECT_COUNTER = new AtomicInteger();
  private TestInfo info;

  @BeforeEach
  void storeTestInfo(TestInfo testInfo) {
    this.info = testInfo;
  }

  protected String getTestName() {
    return info.getTestMethod()
        .map(Method::getName)
        .orElse("unknown");
  }

  /** @return unique lowercase name of maximum 60 characters, including (some of) the current test's name */
  protected String uniqueObjectName() {
    return uniqueObjectName(getTestName());
  }

  /** @return unique lowercase name of maximum 60 characters, including (some of) {@code prefix} */
  public static String uniqueObjectName(String prefix) {
    return Objects.requireNonNull(prefix, "prefix == null")
        .substring(0, Math.min(prefix.length(), 50))
        .toLowerCase(Locale.ROOT)
        + String.format("%010d", OBJECT_COUNTER.getAndIncrement());
  }
}

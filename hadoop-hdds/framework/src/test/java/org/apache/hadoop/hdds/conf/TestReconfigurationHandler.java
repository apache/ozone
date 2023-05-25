/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.conf;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for {@link ReconfigurationHandler}.
 */
class TestReconfigurationHandler {

  private static final String PROP_A = "some.test.property";
  private static final String PROP_B = "other.property";

  private final AtomicReference<String> refA =
      new AtomicReference<>("oldA");
  private final AtomicReference<String> refB =
      new AtomicReference<>("oldB");
  private final ReconfigurationHandler subject =
      new ReconfigurationHandler(new OzoneConfiguration())
          .register(PROP_A, refA::getAndSet)
          .register(PROP_B, refB::getAndSet);

  @Test
  void listProperties() {
    assertEquals(Stream.of(PROP_A, PROP_B).sorted().collect(toList()),
        subject.getReconfigurableProperties());
  }

  @Test
  void callsReconfigurationFunction() {
    subject.reconfigurePropertyImpl(PROP_A, "newA");
    assertEquals("newA", refA.get());

    subject.reconfigurePropertyImpl(PROP_B, "newB");
    assertEquals("newB", refB.get());
  }

  @Test
  void ignoresUnknownProperty() {
    assertDoesNotThrow(() ->
        subject.reconfigurePropertyImpl("foobar", "some value"));
  }

}

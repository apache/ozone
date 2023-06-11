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

import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.ratis.util.function.CheckedConsumer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test for {@link ReconfigurationHandler}.
 */
class TestReconfigurationHandler {

  private static final String PROP_A = "some.test.property";
  private static final String PROP_B = "other.property";
  private static final String COMPRESSION_ENABLED =
      "test.scm.client.compression.enabled";
  private static final String WAIT = "test.scm.client.wait";

  private static final CheckedConsumer<String, IOException> ACCEPT = any -> { };
  private static final CheckedConsumer<String, IOException> DENY = any -> {
    throw new IOException("access denied");
  };

  private final OzoneConfiguration config = new OzoneConfiguration();

  private final AtomicReference<String> refA =
      new AtomicReference<>("oldA");
  private final AtomicReference<String> refB =
      new AtomicReference<>("oldB");
  private final SimpleConfiguration object =
      config.getObject(SimpleConfiguration.class);
  private final AtomicReference<CheckedConsumer<String, IOException>> adminCheck
      = new AtomicReference<>(ACCEPT);

  private final ReconfigurationHandler subject =
      new ReconfigurationHandler(
              "test", config, op -> adminCheck.get().accept(op))
          .register(PROP_A, refA::getAndSet)
          .register(PROP_B, refB::getAndSet)
          .register(object);

  private static Stream<String> expectedReconfigurableProperties() {
    return Stream.of(PROP_A, PROP_B, COMPRESSION_ENABLED, WAIT);
  }

  @Test
  void getProperties() {
    assertEquals(expectedReconfigurableProperties().collect(toSet()),
        subject.getReconfigurableProperties());
  }

  @Test
  void listProperties() throws IOException {
    assertEquals(expectedReconfigurableProperties().sorted().collect(toList()),
        subject.listReconfigureProperties());
  }

  @Test
  void callsReconfigurationFunction() throws ReconfigurationException {
    final String newA = "newA";
    subject.reconfigurePropertyImpl(PROP_A, newA);
    assertEquals(newA, refA.get());

    final String newB = "newB";
    subject.reconfigurePropertyImpl(PROP_B, newB);
    assertEquals(newB, refB.get());

    final boolean newCompressionEnabled = !object.isCompressionEnabled();
    subject.reconfigurePropertyImpl(COMPRESSION_ENABLED,
        Boolean.toString(newCompressionEnabled));
    assertEquals(newCompressionEnabled, object.isCompressionEnabled());

    final long newWaitTime = object.getWaitTime() + 5;
    subject.reconfigurePropertyImpl(WAIT, Long.toString(newWaitTime));
    assertEquals(newWaitTime, object.getWaitTime());
  }

  @Test
  void validatesNewConfiguration() {
    final long oldWaitTime = object.getWaitTime();
    assertThrows(ReconfigurationException.class,
        () -> subject.reconfigurePropertyImpl(WAIT, "0"));
    assertEquals(oldWaitTime, object.getWaitTime());
  }

  @Test
  void ignoresUnknownProperty() {
    assertDoesNotThrow(() ->
        subject.reconfigurePropertyImpl("foobar", "some value"));
  }

  @Test
  void requiresAdminAccess() {
    adminCheck.set(DENY);
    assertThrows(IOException.class, subject::listReconfigureProperties);
    assertThrows(IOException.class, subject::startReconfigure);
    assertThrows(IOException.class, subject::getReconfigureStatus);
  }

}

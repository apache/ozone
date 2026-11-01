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

package org.apache.hadoop.hdds.scm;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link NettyWarmUp} and that {@link XceiverClientCreator} starts it.
 */
class TestNettyWarmUp {

  @BeforeEach
  void reset() {
    NettyWarmUp.reset();
  }

  @AfterEach
  void cleanup() {
    NettyWarmUp.reset();
  }

  @Test
  void startsAtMostOncePerJvm() throws InterruptedException {
    Thread thread = NettyWarmUp.start(false);
    assertNotNull(thread);
    assertTrue(thread.isDaemon());
    thread.join(30_000);
    assertFalse(thread.isAlive(), "The warm-up should finish");

    assertNull(NettyWarmUp.start(true), "The warm-up should start only once per JVM");
  }

  @Test
  void warmsUpTls() throws InterruptedException {
    Thread thread = NettyWarmUp.start(true);
    assertNotNull(thread);
    thread.join(30_000);
    assertFalse(thread.isAlive(), "The TLS warm-up should finish, whether or not netty-tcnative is available");
  }

  @Test
  void creatorStartsWarmUpByDefault() {
    try (XceiverClientCreator ignored = new XceiverClientCreator(new OzoneConfiguration())) {
      assertTrue(NettyWarmUp.isStarted());
    }
  }

  @Test
  void creatorSkipsWarmUpWhenDisabled() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean("ozone.client.netty.warmup.enabled", false);
    try (XceiverClientCreator ignored = new XceiverClientCreator(conf)) {
      assertFalse(NettyWarmUp.isStarted());
    }
  }
}

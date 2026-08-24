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

package org.apache.ozone.erasurecode;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.ozone.erasurecode.rawcoder.RSRawErasureCoderFactory;
import org.junit.jupiter.api.Test;

/**
 * Tests that CodecRegistry does not depend on the thread context class loader
 * (TCCL) for discovering RawErasureCoderFactory providers.
 *
 * <p>CodecRegistry is an eagerly-initialized singleton, so this test must run
 * in its own JVM (e.g. reuseForks=false) as the first test to touch
 * CodecRegistry, otherwise initialization already happened under the normal
 * TCCL and the test passes vacuously.
 */
public class TestCodecRegistryTcclIsolation {

  @Test
  public void testRegistryLoadsWithoutTccl() {
    ClassLoader originalTccl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(new ClassLoader(null) {
      });
      String[] rsCoderNames = CodecRegistry.getInstance()
          .getCoderNames(ECReplicationConfig.EcCodec.RS.name().toLowerCase());
      assertThat(rsCoderNames).isNotNull();
      assertThat(rsCoderNames).isNotEmpty();
      assertThat(rsCoderNames).contains(RSRawErasureCoderFactory.CODER_NAME);
    } finally {
      Thread.currentThread().setContextClassLoader(originalTccl);
    }
  }
}

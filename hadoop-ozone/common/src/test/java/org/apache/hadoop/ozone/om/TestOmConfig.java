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

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class TestOmConfig {

  @Test
  void acceptsValidValues() {
    final long validMaxListSize = 42;
    MutableConfigurationSource conf = new OzoneConfiguration();
    conf.setLong(OmConfig.Keys.SERVER_LIST_MAX_SIZE, validMaxListSize);

    OmConfig subject = conf.getObject(OmConfig.class);

    assertThat(subject.getMaxListSize())
        .isEqualTo(validMaxListSize);
  }

  @ParameterizedTest
  @ValueSource(longs = {-1, 0})
  void overridesInvalidListSize(long invalidValue) {
    MutableConfigurationSource conf = new OzoneConfiguration();
    conf.setLong(OmConfig.Keys.SERVER_LIST_MAX_SIZE, invalidValue);

    OmConfig subject = conf.getObject(OmConfig.class);

    assertThat(subject.getMaxListSize())
        .isEqualTo(OmConfig.Defaults.SERVER_LIST_MAX_SIZE);
  }

}

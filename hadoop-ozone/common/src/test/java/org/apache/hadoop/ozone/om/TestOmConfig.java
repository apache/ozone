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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

  @Test
  void throwsOnInvalidMaxUserVolume() {
    MutableConfigurationSource conf = new OzoneConfiguration();
    conf.setInt(OmConfig.Keys.USER_MAX_VOLUME, 0);

    assertThrows(IllegalArgumentException.class, () -> conf.getObject(OmConfig.class));
  }

  @Test
  void testCopy() {
    MutableConfigurationSource conf = new OzoneConfiguration();
    OmConfig original = conf.getObject(OmConfig.class);

    OmConfig subject = original.copy();

    assertConfigEquals(original, subject);
  }

  @Test
  void testSetFrom() {
    MutableConfigurationSource conf = new OzoneConfiguration();
    OmConfig subject = conf.getObject(OmConfig.class);
    OmConfig updated = conf.getObject(OmConfig.class);
    updated.setFileSystemPathEnabled(!updated.isFileSystemPathEnabled());
    updated.setKeyNameCharacterCheckEnabled(!updated.isKeyNameCharacterCheckEnabled());
    updated.setMaxListSize(updated.getMaxListSize() + 1);
    updated.setMaxUserVolumeCount(updated.getMaxUserVolumeCount() + 1);

    subject.setFrom(updated);

    assertConfigEquals(updated, subject);
  }

  private static void assertConfigEquals(OmConfig expected, OmConfig actual) {
    assertEquals(expected.getMaxListSize(), actual.getMaxListSize());
    assertEquals(expected.isFileSystemPathEnabled(), actual.isFileSystemPathEnabled());
    assertEquals(expected.isKeyNameCharacterCheckEnabled(), actual.isKeyNameCharacterCheckEnabled());
    assertEquals(expected.getMaxUserVolumeCount(), actual.getMaxUserVolumeCount());
  }

}

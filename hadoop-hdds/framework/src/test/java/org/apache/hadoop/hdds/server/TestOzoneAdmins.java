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

package org.apache.hadoop.hdds.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * This class is to test the utilities present in the OzoneAdmins class.
 */
class TestOzoneAdmins {
  // The following set of tests are to validate the S3 based utilities present in OzoneAdmins
  private OzoneConfiguration configuration;

  @BeforeEach
  void setUp() {
    configuration = new OzoneConfiguration();
  }

  @ParameterizedTest
  @ValueSource(strings = {OzoneConfigKeys.OZONE_S3_ADMINISTRATORS,
                          OzoneConfigKeys.OZONE_ADMINISTRATORS})
  void testS3AdminExtraction(String configKey) throws IOException {
    configuration.set(configKey, "alice,bob");

    assertThat(OzoneAdmins.getS3AdminsFromConfig(configuration))
        .containsAll(Arrays.asList("alice", "bob"));
  }

  @ParameterizedTest
  @ValueSource(strings = {OzoneConfigKeys.OZONE_S3_ADMINISTRATORS_GROUPS,
                          OzoneConfigKeys.OZONE_ADMINISTRATORS_GROUPS})
  void testS3AdminGroupExtraction(String configKey) {
    configuration.set(configKey, "test1, test2");

    assertThat(OzoneAdmins.getS3AdminsGroupsFromConfig(configuration))
        .containsAll(Arrays.asList("test1", "test2"));
  }

  @ParameterizedTest
  @CsvSource({
      OzoneConfigKeys.OZONE_ADMINISTRATORS + ", " +  OzoneConfigKeys.OZONE_ADMINISTRATORS_GROUPS,
      OzoneConfigKeys.OZONE_S3_ADMINISTRATORS + ", " + OzoneConfigKeys.OZONE_S3_ADMINISTRATORS_GROUPS
  })
  void testIsAdmin(String adminKey, String adminGroupKey) {
    // When there is no S3 admin, but Ozone admins present
    configuration.set(adminKey, "alice");
    configuration.set(adminGroupKey, "test_group");

    OzoneAdmins admins = OzoneAdmins.getS3Admins(configuration);
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        "alice", new String[] {"test_group"});

    assertThat(admins.isAdmin(ugi)).isEqualTo(true);

    // Test that when a user is present in an admin group but not an Ozone Admin
    UserGroupInformation ugiGroupOnly = UserGroupInformation.createUserForTesting(
        "bob", new String[] {"test_group"});
    assertThat(admins.isAdmin(ugiGroupOnly)).isEqualTo(true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testIsAdminWithUgi(boolean isAdminSet) {
    if (isAdminSet) {
      configuration.set(OzoneConfigKeys.OZONE_ADMINISTRATORS, "alice");
      configuration.set(OzoneConfigKeys.OZONE_ADMINISTRATORS_GROUPS, "test_group");
    }
    OzoneAdmins admins = OzoneAdmins.getS3Admins(configuration);
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        "alice", new String[] {"test_group"});
    // Test that when a user is present in an admin group but not an Ozone Admin
    UserGroupInformation ugiGroupOnly = UserGroupInformation.createUserForTesting(
        "bob", new String[] {"test_group"});

    assertThat(admins.isAdmin(ugi)).isEqualTo(isAdminSet);
    assertThat(admins.isAdmin(ugiGroupOnly)).isEqualTo(isAdminSet);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testIsS3AdminWithUgiAndConfiguration(boolean isAdminSet) {
    if (isAdminSet) {
      configuration.set(OzoneConfigKeys.OZONE_S3_ADMINISTRATORS, "alice");
      configuration.set(OzoneConfigKeys.OZONE_S3_ADMINISTRATORS_GROUPS, "test_group");
      UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
          "alice", new String[] {"test_group"});
      // Scenario when user is present in an admin group but not an Ozone Admin
      UserGroupInformation ugiGroupOnly = UserGroupInformation.createUserForTesting(
          "bob", new String[] {"test_group"});

      assertThat(OzoneAdmins.isS3Admin(ugi, configuration)).isEqualTo(true);
      assertThat(OzoneAdmins.isS3Admin(ugiGroupOnly, configuration)).isEqualTo(true);
    } else {
      assertThat(OzoneAdmins.isS3Admin(null, configuration)).isEqualTo(false);
    }

  }
}

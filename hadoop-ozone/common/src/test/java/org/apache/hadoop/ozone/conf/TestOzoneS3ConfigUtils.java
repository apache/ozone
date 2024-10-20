/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.ozone.conf;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This class is to test S3 configuration based utils.
 */
class TestOzoneS3ConfigUtils {

  @Test
  void testS3AdminExtraction() throws IOException {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OzoneConfigKeys.OZONE_S3_ADMINISTRATORS, "alice,bob");

    assertThat(OzoneS3ConfigUtils.getS3AdminsFromConfig(configuration))
        .containsAll(Arrays.asList("alice", "bob"));
  }

  @Test
  void testS3AdminExtractionWithFallback() throws IOException {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OzoneConfigKeys.OZONE_ADMINISTRATORS, "alice,bob");

    assertThat(OzoneS3ConfigUtils.getS3AdminsFromConfig(configuration))
        .containsAll(Arrays.asList("alice", "bob"));
  }

  @Test
  void testS3AdminGroupExtraction() {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OzoneConfigKeys.OZONE_S3_ADMINISTRATORS_GROUPS,
        "test1, test2");

    assertThat(OzoneS3ConfigUtils.getS3AdminsGroupsFromConfig(configuration))
        .containsAll(Arrays.asList("test1", "test2"));
  }

  @Test
  void testS3AdminGroupExtractionWithFallback() {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OzoneConfigKeys.OZONE_ADMINISTRATORS_GROUPS,
        "test1, test2");

    assertThat(OzoneS3ConfigUtils.getS3AdminsGroupsFromConfig(configuration))
        .containsAll(Arrays.asList("test1", "test2"));
  }

  @Test
  void testGetS3AdminsWhenS3AdminPresent() {
    // When there is S3 admin present
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OzoneConfigKeys.OZONE_S3_ADMINISTRATORS, "alice");
    configuration.set(OzoneConfigKeys.OZONE_S3_ADMINISTRATORS_GROUPS, "test_group");

    OzoneAdmins admins = OzoneS3ConfigUtils.getS3Admins(configuration);
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        "alice", new String[] {"test_group"});

    assertThat(admins.isAdmin(ugi)).isEqualTo(true);

    // Test that when a user is present in an admin group but not an Ozone Admin
    UserGroupInformation ugiGroupOnly = UserGroupInformation.createUserForTesting(
        "bob", new String[] {"test_group"});
    assertThat(admins.isAdmin(ugiGroupOnly)).isEqualTo(true);
  }
  @Test
  void testGetS3AdminsWhenNoS3AdminPresent() {
    // When there is no S3 admin, but Ozone admins present
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OzoneConfigKeys.OZONE_ADMINISTRATORS, "alice");
    configuration.set(OzoneConfigKeys.OZONE_ADMINISTRATORS_GROUPS, "test_group");

    OzoneAdmins admins = OzoneS3ConfigUtils.getS3Admins(configuration);
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        "alice", new String[] {"test_group"});

    assertThat(admins.isAdmin(ugi)).isEqualTo(true);

    // Test that when a user is present in an admin group but not an Ozone Admin
    UserGroupInformation ugiGroupOnly = UserGroupInformation.createUserForTesting(
        "bob", new String[] {"test_group"});
    assertThat(admins.isAdmin(ugiGroupOnly)).isEqualTo(true);
  }

  @Test
  void testGetS3AdminsWithNoAdmins() {
    OzoneAdmins admins = OzoneS3ConfigUtils.getS3Admins(new OzoneConfiguration());
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        "alice", new String[] {"test_group"});
    assertThat(admins.isAdmin(ugi)).isEqualTo(false);
  }

  @Test
  void testIsS3AdminForS3AdminUser() {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OzoneConfigKeys.OZONE_S3_ADMINISTRATORS, "alice");
    configuration.set(OzoneConfigKeys.OZONE_S3_ADMINISTRATORS_GROUPS, "test_group");

    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        "alice", new String[] {"test_group"});
    // Scenario when user is present in an admin group but not an Ozone Admin
    UserGroupInformation ugiGroupOnly = UserGroupInformation.createUserForTesting(
        "bob", new String[] {"test_group"});

    assertThat(OzoneS3ConfigUtils.isS3Admin(ugi, configuration)).isEqualTo(true);
    assertThat(OzoneS3ConfigUtils.isS3Admin(ugiGroupOnly, configuration)).isEqualTo(true);
  }

  @Test
  void testIsS3AdminForAdminUser() {
    // When there is no S3 admin, but Ozone admins present
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OzoneConfigKeys.OZONE_ADMINISTRATORS, "alice");
    configuration.set(OzoneConfigKeys.OZONE_ADMINISTRATORS_GROUPS, "test_group");

    OzoneAdmins admins = OzoneS3ConfigUtils.getS3Admins(configuration);
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        "alice", new String[] {"test_group"});
    // Test that when a user is present in an admin group but not an Ozone Admin
    UserGroupInformation ugiGroupOnly = UserGroupInformation.createUserForTesting(
        "bob", new String[] {"test_group"});

    assertThat(admins.isAdmin(ugi)).isEqualTo(true);
    assertThat(admins.isAdmin(ugiGroupOnly)).isEqualTo(true);
  }

  @Test
  void testIsS3AdminForNoUser() {
    OzoneConfiguration configuration = new OzoneConfiguration();
    assertThat(OzoneS3ConfigUtils.isS3Admin(null, configuration)).isEqualTo(false);
  }
}

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

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.FileSystem.TRASH_PREFIX;
import static org.apache.hadoop.fs.ozone.OzoneTrashPolicy.CURRENT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_DELETE_BATCH_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OmConfig.Keys.ENABLE_FILESYSTEM_PATHS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.FILE_SYSTEM_OPTIMIZED;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.OBJECT_STORE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.DBConfigFromFile;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.FaultInjectorImpl;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.OzoneTrash;
import org.apache.hadoop.ozone.om.ScmBlockLocationTestingClient;
import org.apache.hadoop.ozone.om.TrashOzoneFileSystem;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmLCExpiration;
import org.apache.hadoop.ozone.om.helpers.OmLCFilter;
import org.apache.hadoop.ozone.om.helpers.OmLCRule;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleConfiguration;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleRuleAndOperator;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.key.OMKeysDeleteRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleConfiguration;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.OzoneTestBase;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Test Key Lifecycle Service.
 * <p>
 * This test does the following things.
 * <p>
 * 1. Creates a bunch of keys.
 * 2. Then executes delete key directly using Metadata Manager.
 * 3. Waits for a while for the KeyDeleting Service to pick up and call into SCM.
 * 4. Confirms that calls have been successful.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(300)
class TestKeyLifecycleService extends OzoneTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestKeyLifecycleService.class);
  private static final AtomicInteger OBJECT_COUNTER = new AtomicInteger();
  private static final AtomicInteger OBJECT_ID_COUNTER = new AtomicInteger();
  private static final int KEY_COUNT = 2;
  private static final int EXPIRE_SECONDS = 2;
  private static final int SERVICE_INTERVAL = 300;
  private static final int WAIT_CHECK_INTERVAL = 50;

  private OzoneConfiguration conf;
  private OzoneManagerProtocol writeClient;
  private OzoneManager om;
  private KeyManager keyManager;
  private OMMetadataManager metadataManager;
  private KeyLifecycleService keyLifecycleService;
  private KeyDeletingService keyDeletingService;
  private DirectoryDeletingService directoryDeletingService;
  private ScmBlockLocationTestingClient scmBlockTestingClient;
  private KeyLifecycleServiceMetrics metrics;
  private long bucketObjectID;

  @BeforeAll
  void setup() {
    ExitUtils.disableSystemExit();
  }

  private void createConfig(File testDir) {
    conf = new OzoneConfiguration();
    System.setProperty(DBConfigFromFile.CONFIG_DIR, "/");
    ServerUtils.setOzoneMetaDirPath(conf, testDir.toString());
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL,
        200, TimeUnit.MILLISECONDS);
    conf.setBoolean(OZONE_KEY_LIFECYCLE_SERVICE_ENABLED, true);
    conf.setTimeDuration(OZONE_KEY_LIFECYCLE_SERVICE_INTERVAL, SERVICE_INTERVAL, TimeUnit.MILLISECONDS);
    conf.setInt(OZONE_KEY_LIFECYCLE_SERVICE_DELETE_BATCH_SIZE, 50);
    conf.setQuietMode(false);
    conf.setBoolean(ENABLE_FILESYSTEM_PATHS, false);
    OmLCExpiration.setTest(true);
  }

  private void createSubject() throws Exception {
    OmTestManagers omTestManagers = new OmTestManagers(conf, scmBlockTestingClient, null);
    keyManager = omTestManagers.getKeyManager();
    keyLifecycleService = keyManager.getKeyLifecycleService();
    metrics = keyLifecycleService.getMetrics();
    keyDeletingService = keyManager.getDeletingService();
    directoryDeletingService = keyManager.getDirDeletingService();
    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();
    metadataManager = omTestManagers.getMetadataManager();
  }

  /**
   * Tests happy path.
   */
  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class Normal {

    @BeforeAll
    void setup(@TempDir File testDir) throws Exception {
      // failCallsFrequency = 0 means all calls succeed
      scmBlockTestingClient = new ScmBlockLocationTestingClient(null, null, 0);

      createConfig(testDir);
      createSubject();
      keyDeletingService.suspend();
      directoryDeletingService.suspend();
    }

    @AfterEach
    void resume() {
      keyLifecycleService.setOzoneTrash(null);
      keyLifecycleService.setMoveToTrashEnabled(true);
    }

    @AfterAll
    void cleanup() {
      if (om != null) {
        om.stop();
        om.join();
      }
    }

    public Stream<Arguments> parameters1() {
      return Stream.of(
          arguments(FILE_SYSTEM_OPTIMIZED, true),
          arguments(FILE_SYSTEM_OPTIMIZED, false),
          arguments(BucketLayout.OBJECT_STORE, true),
          arguments(BucketLayout.OBJECT_STORE, false)
      );
    }

    /**
     * In this test, we create a bunch of keys and a lifecycle configuration. Then we start the
     * KeyLifecycleService and make sure that all the keys that expired is picked up and
     * moved to delete table.
     */
    @ParameterizedTest
    @MethodSource("parameters1")
    void testAllKeyExpired(BucketLayout bucketLayout, boolean createPrefix) throws IOException,
        TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, prefix, null);
      // check there are keys in keyTable
      assertEquals(KEY_COUNT, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      if (createPrefix) {
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, prefix, null, date.toString(), true);
      } else {
        OmLCFilter.Builder filter = getOmLCFilterBuilder(prefix, null, null);
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, null, filter.build(), date.toString(), true);
      }

      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @MethodSource("parameters1")
    void testOneKeyExpired(BucketLayout bucketLayout, boolean createPrefix) throws IOException,
        TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, prefix, null);
      // check there are keys in keyTable
      assertEquals(KEY_COUNT, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      int keyIndex = ThreadLocalRandom.current().nextInt(KEY_COUNT - 1);
      if (createPrefix) {
        createLifecyclePolicy(volumeName, bucketName, bucketLayout,
            keyList.get(keyIndex).getKeyName(), null, date.toString(), true);
      } else {
        OmLCFilter.Builder filter = getOmLCFilterBuilder(keyList.get(keyIndex).getKeyName(), null, null);
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, null, filter.build(), date.toString(), true);
      }

      GenericTestUtils.waitFor(() -> (getDeletedKeyCount() - initialDeletedKeyCount) == 1, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(KEY_COUNT - 1, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @MethodSource("parameters1")
    void testOnlyKeyExpired(BucketLayout bucketLayout, boolean createPrefix) throws IOException,
        TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);

      // Create the key
      createVolumeAndBucket(volumeName, bucketName, bucketLayout,
          UserGroupInformation.getCurrentUser().getShortUserName());
      OmKeyArgs keyArg = createAndCommitKey(volumeName, bucketName, uniqueObjectName(prefix), 1, null);

      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      if (createPrefix) {
        createLifecyclePolicy(volumeName, bucketName, bucketLayout,
            keyArg.getKeyName(), null, date.toString(), true);
      } else {
        OmLCFilter.Builder filter = getOmLCFilterBuilder(keyArg.getKeyName(), null, null);
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, null, filter.build(), date.toString(), true);
      }

      GenericTestUtils.waitFor(() -> (getDeletedKeyCount() - initialDeletedKeyCount) == 1, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
    void testAllKeyExpiredWithTag(BucketLayout bucketLayout) throws IOException,
        TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      Pair<String, String> tag = Pair.of("app", "spark");
      Map<String, String> tags = ImmutableMap.of("app", "spark");
      // create keys with tags
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, prefix, tags);
      // check there are keys in keyTable
      assertEquals(KEY_COUNT, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      OmLCFilter.Builder filter = getOmLCFilterBuilder(null, tag, null);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, null, filter.build(), date.toString(), true);

      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
    void testOneKeyExpiredWithTag(BucketLayout bucketLayout) throws IOException,
        TimeoutException, InterruptedException {
      int keyCount = KEY_COUNT;
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      Pair<String, String> tag = Pair.of("app", "spark");
      Map<String, String> tags = ImmutableMap.of("app", "spark");
      // create keys without tag
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, keyCount, 1, prefix, null);
      // create one more key with tag
      final String keyName = uniqueObjectName(prefix);
      // Create the key
      OmKeyArgs keyArg = createAndCommitKey(volumeName, bucketName, keyName, 1, tags);
      keyList.add(keyArg);
      keyCount++;

      // check there are keys in keyTable
      assertEquals(keyCount, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT + 1,
          WAIT_CHECK_INTERVAL, 1000);
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      OmLCFilter.Builder filter = getOmLCFilterBuilder(null, tag, null);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, null, filter.build(), date.toString(), true);

      GenericTestUtils.waitFor(() -> (getDeletedKeyCount() - initialDeletedKeyCount) == 1, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(keyCount - 1, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
    void testAllKeyExpiredWithAndOperator(BucketLayout bucketLayout) throws IOException,
        TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      Map<String, String> tags = ImmutableMap.of("app", "spark", "user", "ozone");
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, prefix, tags);
      // check there are keys in keyTable
      assertEquals(KEY_COUNT, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      OmLifecycleRuleAndOperator andOperator = getOmLCAndOperatorBuilder(prefix, tags).build();
      OmLCFilter.Builder filter = getOmLCFilterBuilder(null, null, andOperator);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, null, filter.build(), date.toString(), true);

      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
    void testOneKeyExpiredWithAndOperator(BucketLayout bucketLayout) throws IOException,
        TimeoutException, InterruptedException {
      int keyCount = KEY_COUNT;
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      Map<String, String> tags = ImmutableMap.of("app", "spark", "user", "ozone");
      // create keys without tags
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, keyCount, 1, prefix, null);
      // create one more key with tag
      final String keyName = uniqueObjectName(prefix);
      // Create the key
      OmKeyArgs keyArg = createAndCommitKey(volumeName, bucketName, keyName, 1, tags);
      keyList.add(keyArg);
      keyCount++;

      // check there are keys in keyTable
      assertEquals(keyCount, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT + 1,
          WAIT_CHECK_INTERVAL, 1000);
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      OmLifecycleRuleAndOperator andOperator = getOmLCAndOperatorBuilder(prefix, tags).build();
      OmLCFilter.Builder filter = getOmLCFilterBuilder(null, null, andOperator);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, null, filter.build(), date.toString(), true);

      GenericTestUtils.waitFor(() -> (getDeletedKeyCount() - initialDeletedKeyCount) == 1, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(keyCount - 1, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
    void testEmptyPrefix(BucketLayout bucketLayout) throws IOException, TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, prefix, null);
      // check there are keys in keyTable
      assertEquals(KEY_COUNT, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, "", null, date.toString(), true);

      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
    void testEmptyFilter(BucketLayout bucketLayout) throws IOException,
        TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, prefix, null);
      // check there are keys in keyTable
      assertEquals(KEY_COUNT, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      OmLCFilter.Builder filter = getOmLCFilterBuilder(null, null, null);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, null, filter.build(), date.toString(), true);

      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    public Stream<Arguments> parameters2() {
      return Stream.of(
          arguments(BucketLayout.OBJECT_STORE, "/"),
          arguments(BucketLayout.LEGACY, "/")
      );
    }

    @ParameterizedTest
    @MethodSource("parameters2")
    void testRootSlashPrefix(BucketLayout bucketLayout, String prefix)
        throws IOException, TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String keyPrefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, keyPrefix, null);
      // check there are keys in keyTable
      assertEquals(KEY_COUNT, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, prefix, null, date.toString(), true);

      if (bucketLayout == FILE_SYSTEM_OPTIMIZED) {
        GenericTestUtils.waitFor(() ->
            (getDeletedKeyCount() - initialDeletedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 10000);
        assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);
      } else {
        Thread.sleep(EXPIRE_SECONDS);
        assertEquals(0, getDeletedKeyCount() - initialDeletedKeyCount);
        assertEquals(KEY_COUNT, getKeyCount(bucketLayout) - initialKeyCount);
      }
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @MethodSource("parameters1")
    void testSlashPrefix(BucketLayout bucketLayout, boolean createPrefix)
        throws IOException, TimeoutException, InterruptedException {
      assumeTrue(bucketLayout != FILE_SYSTEM_OPTIMIZED);
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String keyPrefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, keyPrefix, null);
      // check there are keys in keyTable
      assertEquals(KEY_COUNT, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      if (createPrefix) {
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, "/" + keyPrefix, null, date.toString(), true);
      } else {
        OmLCFilter.Builder filter = getOmLCFilterBuilder("/" + keyPrefix, null, null);
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, null, filter.build(), date.toString(), true);
      }

      if (bucketLayout == FILE_SYSTEM_OPTIMIZED) {
        GenericTestUtils.waitFor(() ->
            (getDeletedKeyCount() - initialDeletedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 10000);
        assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);
      } else {
        Thread.sleep(EXPIRE_SECONDS);
        assertEquals(0, getDeletedKeyCount() - initialDeletedKeyCount);
        assertEquals(KEY_COUNT, getKeyCount(bucketLayout) - initialKeyCount);
      }
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @MethodSource("parameters1")
    void testSlashKey(BucketLayout bucketLayout, boolean createPrefix)
        throws IOException, TimeoutException, InterruptedException {
      assumeTrue(bucketLayout != FILE_SYSTEM_OPTIMIZED);
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String keyPrefix = "/key//";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, keyPrefix, null);
      // check there are keys in keyTable
      assertEquals(KEY_COUNT, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      if (createPrefix) {
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, keyPrefix, null, date.toString(), true);
      } else {
        OmLCFilter.Builder filter = getOmLCFilterBuilder(keyPrefix, null, null);
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, null, filter.build(), date.toString(), true);
      }

      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
    void testSlashKeyWithAndOperator(BucketLayout bucketLayout)
        throws IOException, TimeoutException, InterruptedException {
      assumeTrue(bucketLayout != FILE_SYSTEM_OPTIMIZED);
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String keyPrefix = "/key//";
      Map<String, String> tags = ImmutableMap.of("app", "spark", "user", "ozone");
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, keyPrefix, tags);
      // check there are keys in keyTable
      assertEquals(KEY_COUNT, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      OmLifecycleRuleAndOperator andOperator = getOmLCAndOperatorBuilder(keyPrefix, tags).build();
      OmLCFilter.Builder filter = getOmLCFilterBuilder(null, null, andOperator);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, null, filter.build(), date.toString(), true);

      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
    void testSlashPrefixWithAndOperator(BucketLayout bucketLayout)
        throws IOException, InterruptedException, TimeoutException {
      assumeTrue(bucketLayout != FILE_SYSTEM_OPTIMIZED);
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String keyPrefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      Map<String, String> tags = ImmutableMap.of("app", "spark", "user", "ozone");
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, keyPrefix, tags);
      // check there are keys in keyTable
      assertEquals(KEY_COUNT, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      OmLifecycleRuleAndOperator andOperator = getOmLCAndOperatorBuilder("/" + keyPrefix, tags).build();
      OmLCFilter.Builder filter = getOmLCFilterBuilder(null, null, andOperator);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, null, filter.build(), date.toString(), true);

      Thread.sleep(EXPIRE_SECONDS);
      assertEquals(0, getDeletedKeyCount() - initialDeletedKeyCount);
      assertEquals(KEY_COUNT, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @EnumSource(BucketLayout.class)
    void testComplexPrefix(BucketLayout bucketLayout) throws IOException,
        TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "dir1/dir2/dir3/key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      long initialNumDeletedKey = metrics.getNumKeyDeleted().value();
      long initialSizeDeletedKey = metrics.getSizeKeyDeleted().value();
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, prefix, null);
      // check there are keys in keyTable
      assertEquals(KEY_COUNT, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, prefix, null, date.toString(), true);

      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);
      assertEquals(KEY_COUNT, metrics.getNumKeyDeleted().value() - initialNumDeletedKey);
      // each key is 1000 bytes size
      assertEquals(1000 * 3 * KEY_COUNT, metrics.getSizeKeyDeleted().value() - initialSizeDeletedKey);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
    void testPrefixNotMatch(BucketLayout bucketLayout) throws IOException, InterruptedException, TimeoutException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String keyPrefix = "dir1/dir2/dir3/key";
      String filterPrefix = "dir1/dir2/dir4/key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, keyPrefix, null);
      // check there are keys in keyTable
      assertEquals(KEY_COUNT, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      OmLCFilter.Builder filter = getOmLCFilterBuilder(filterPrefix, null, null);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, null, filter.build(), date.toString(), true);

      Thread.sleep(EXPIRE_SECONDS);
      assertEquals(0, getDeletedKeyCount() - initialDeletedKeyCount);
      assertEquals(KEY_COUNT, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
    void testExpireKeysUnderDirectory(BucketLayout bucketLayout) throws IOException,
        TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "dir1/dir2/dir3/key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, prefix, null);
      // check there are keys in keyTable
      assertEquals(KEY_COUNT, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);
      // assert directory "dir1/dir2/dir3" exists
      if (bucketLayout == FILE_SYSTEM_OPTIMIZED) {
        KeyInfoWithVolumeContext keyInfo = getDirectory(volumeName, bucketName, "dir1/dir2/dir3");
        assertFalse(keyInfo.getKeyInfo().isFile());
      }

      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, prefix, null, date.toString(), true);

      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    public Stream<Arguments> parameters3() {
      return Stream.of(
          arguments("dir1/dir2/dir3/key", "dir1/dir2/dir3/", "dir1/dir2/dir3"),
          arguments("dir1/dir2/dir3/key", "dir1/dir2/dir3", "dir1/dir2/dir3"),
          arguments("dir1/key", "dir1", "dir1"),
          arguments("dir1/key", "dir1/", "dir1"));
    }

    @ParameterizedTest
    @MethodSource("parameters3")
    void testMatchedDirectoryNotDeleted(String keyPrefix, String rulePrefix, String dirName) throws IOException,
        TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialDeletedDirCount = getDeletedDirectoryCount();
      long initialKeyCount = getKeyCount(FILE_SYSTEM_OPTIMIZED);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, FILE_SYSTEM_OPTIMIZED, KEY_COUNT, 1, keyPrefix, null);
      // check there are keys in keyTable
      assertEquals(KEY_COUNT, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(FILE_SYSTEM_OPTIMIZED) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);

      // assert directory exists
      KeyInfoWithVolumeContext keyInfo = getDirectory(volumeName, bucketName, dirName);
      assertFalse(keyInfo.getKeyInfo().isFile());

      KeyLifecycleService.setInjectors(
          Arrays.asList(new FaultInjectorImpl(), new FaultInjectorImpl()));

      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, FILE_SYSTEM_OPTIMIZED, rulePrefix, null, date.toString(), true);
      LOG.info("expiry date {}", date.toInstant().toEpochMilli());

      GenericTestUtils.waitFor(() -> date.isBefore(ZonedDateTime.now(ZoneOffset.UTC)), WAIT_CHECK_INTERVAL, 10000);

      // rename a key under directory to change directory's Modification time
      writeClient.renameKey(keyList.get(0), keyList.get(0).getKeyName() + "-new");
      LOG.info("Dir {} refreshes its modification time", dirName);

      // resume KeyLifecycleService bucket scan
      KeyLifecycleService.getInjector(0).resume();
      KeyLifecycleService.getInjector(1).resume();

      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(0, getKeyCount(FILE_SYSTEM_OPTIMIZED) - initialKeyCount);
      assertEquals(0, getDeletedDirectoryCount() - initialDeletedDirCount);
      KeyInfoWithVolumeContext directory = getDirectory(volumeName, bucketName, dirName);
      assertNotNull(directory);

      deleteLifecyclePolicy(volumeName, bucketName);
    }

    public Stream<Arguments> parameters4() {
      return Stream.of(
          arguments("dir1/dir2/dir3", "dir1/dir2/dir3", 3, 0, true, false),
          arguments("dir1/dir2/dir3", "dir1/dir2/dir3", 3, 0, false, true),
          arguments("/dir1/dir2/dir3", "dir1/dir2/dir3", 3, 1, true, false),
          arguments("/dir1/dir2/dir3", "dir1/dir2/dir3", 3, 1, false, true),
          arguments("/dir1/dir2/dir3/", "dir1/dir2/dir3/", 3, 0, true, false),
          arguments("/dir1/dir2/dir3/", "dir1/dir2/dir3/", 3, 0, false, true),
          arguments("/dir1//dir2//dir3//", "dir1/dir2/dir3/", 3, 0, true, false),
          arguments("/dir1//dir2//dir3//", "dir1/dir2/dir3/", 3, 0, false, true),
          arguments("/dir1//dir2/dir3/", "dir1/dir2/dir", 3, 1, true, false),
          arguments("/dir1//dir2/dir3/", "dir1/dir2/dir", 3, 1, false, true),
          arguments("/dir1//dir2/dir3/", "dir1/dir2/dir/", 3, 0, true, false),
          arguments("/dir1//dir2/dir3/", "dir1/dir2/dir/", 3, 0, false, true),
          arguments("dir1/dir2", "dir1/dir2/", 2, 0, true, false),
          arguments("dir1/dir2", "dir1/dir2/", 2, 0, false, true),
          arguments("/dir1/dir2", "dir1/dir2", 2, 1, true, false),
          arguments("/dir1/dir2", "dir1/dir2", 2, 1, false, true),
          arguments("/dir1/dir2/", "dir1/dir2", 2, 1, true, false),
          arguments("/dir1/dir2/", "dir1/dir2", 2, 1, false, true),
          arguments("/dir1//dir2//", "dir1/dir2/", 2, 0, true, false),
          arguments("/dir1//dir2//", "dir1/dir2/", 2, 0, false, true),
          arguments("/dir1//dir2//", "dir1/dir", 2, 1, true, false),
          arguments("/dir1//dir2//", "dir1/dir", 2, 1, false, true),
          arguments("/dir1//dir2//", "dir1/dir/", 2, 0, true, false),
          arguments("/dir1//dir2//", "dir1/dir/", 2, 0, false, true),
          arguments("dir1", "dir1/", 1, 0, true, false),
          arguments("dir1", "dir1/", 1, 0, false, true),
          arguments("/dir1", "dir1", 1, 1, true, false),
          arguments("/dir1", "dir1", 1, 1, false, true),
          arguments("/dir1/", "dir1", 1, 1, true, false),
          arguments("/dir1/", "dir1", 1, 1, false, true),
          arguments("/dir1//", "dir1/", 1, 0, true, false),
          arguments("/dir1//", "dir1/", 1, 0, false, true),
          arguments("/dir1//", "dir", 1, 1, true, false),
          arguments("/dir1//", "dir", 1, 1, false, true),
          arguments("/dir1//", "dir/", 1, 0, true, false),
          arguments("/dir1//", "dir/", 1, 0, false, true)
      );
    }

    @ParameterizedTest
    @MethodSource("parameters4")
    void testExpireOnlyDirectory(String dirName, String prefix, int dirDepth, int deletedDirCount,
        boolean createPrefix, boolean createFilterPrefix) throws IOException,
        TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      long initialDeletedDirCount = getDeletedDirectoryCount();
      long initialDirCount = getDirCount();
      long initialNumDeletedDir = metrics.getNumDirDeleted().value();

      // Create the directory
      createVolumeAndBucket(volumeName, bucketName, FILE_SYSTEM_OPTIMIZED,
          UserGroupInformation.getCurrentUser().getShortUserName());
      createDirectory(volumeName, bucketName, dirName);
      KeyInfoWithVolumeContext keyInfo = getDirectory(volumeName, bucketName, dirName);
      assertFalse(keyInfo.getKeyInfo().isFile());
      Thread.sleep(SERVICE_INTERVAL);
      assertEquals(dirDepth, getDirCount() - initialDirCount);
      assertEquals(0, getDeletedDirectoryCount() - initialDeletedDirCount);

      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      if (createPrefix) {
        createLifecyclePolicy(volumeName, bucketName, FILE_SYSTEM_OPTIMIZED,
            prefix, null, date.toString(), true);
      } else if (createFilterPrefix) {
        OmLCFilter.Builder filter = getOmLCFilterBuilder(prefix, null, null);
        createLifecyclePolicy(volumeName, bucketName, FILE_SYSTEM_OPTIMIZED,
            null, filter.build(), date.toString(), true);
      }

      GenericTestUtils.waitFor(
          () -> (getDeletedDirectoryCount() - initialDeletedDirCount) == deletedDirCount, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(dirDepth - deletedDirCount, getDirCount() - initialDirCount);
      assertEquals(deletedDirCount, metrics.getNumDirDeleted().value() - initialNumDeletedDir);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
    void testExpireNonExistDirectory(BucketLayout bucketLayout)
        throws IOException, InterruptedException, TimeoutException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "dir1/dir2/dir3/key";
      String dirPath = "dir1/dir2/dir3";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      long initialDeletedDirCount = getDeletedDirectoryCount();
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, prefix, null);
      // check there are keys in keyTable
      assertEquals(KEY_COUNT, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);
      // assert directory "dir1/dir2/dir3" exists
      if (bucketLayout == FILE_SYSTEM_OPTIMIZED) {
        KeyInfoWithVolumeContext keyInfo = getDirectory(volumeName, bucketName, dirPath);
        assertFalse(keyInfo.getKeyInfo().isFile());
      }

      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, "dir1/dir2/dir4", null, date.toString(), true);

      Thread.sleep(EXPIRE_SECONDS);

      if (bucketLayout == FILE_SYSTEM_OPTIMIZED) {
        assertEquals(0, getDeletedDirectoryCount() - initialDeletedDirCount);
        assertEquals(0, getDeletedKeyCount() - initialDeletedKeyCount);
      } else {
        assertEquals(0, getDeletedKeyCount() - initialDeletedKeyCount);
      }
      assertEquals(KEY_COUNT, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
    void testRuleDisabled(BucketLayout bucketLayout) throws IOException, InterruptedException, TimeoutException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, prefix, null);
      // check there are keys in keyTable
      assertEquals(KEY_COUNT, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, "", null, date.toString(), false);
      Thread.sleep(EXPIRE_SECONDS);
      assertEquals(initialDeletedKeyCount, getDeletedKeyCount());
      assertEquals(KEY_COUNT, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
    void testOneRuleDisabledOneRuleEnabled(BucketLayout bucketLayout)
        throws IOException, InterruptedException, TimeoutException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, prefix, null);
      // check there are keys in keyTable
      assertEquals(KEY_COUNT, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      List<OmLCRule> ruleList = new ArrayList<>();
      ruleList.add(new OmLCRule.Builder().setId(String.valueOf(OBJECT_ID_COUNTER.getAndIncrement()))
          .setEnabled(false).setPrefix(prefix)
          .setAction(new OmLCExpiration.Builder().setDate(date.toString()).build())
          .build());
      ruleList.add(new OmLCRule.Builder().setId(String.valueOf(OBJECT_ID_COUNTER.getAndIncrement()))
          .setEnabled(true).setPrefix(prefix)
          .setAction(new OmLCExpiration.Builder().setDate(date.toString()).build())
          .build());
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, ruleList);

      GenericTestUtils.waitFor(
          () -> (getDeletedKeyCount() - initialDeletedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
    void testKeyUpdatedShouldNotGetDeleted(BucketLayout bucketLayout)
        throws IOException, InterruptedException, TimeoutException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      GenericTestUtils.LogCapturer log =
          GenericTestUtils.LogCapturer.captureLogs(
              LoggerFactory.getLogger(KeyLifecycleService.class));
      GenericTestUtils.LogCapturer requestLog =
          GenericTestUtils.LogCapturer.captureLogs(
              LoggerFactory.getLogger(OMKeysDeleteRequest.class));
      String prefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, prefix, null);

      KeyLifecycleService.setInjectors(
          Arrays.asList(new FaultInjectorImpl(), new FaultInjectorImpl()));

      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, prefix, null, date.toString(), true);
      Thread.sleep(SERVICE_INTERVAL);
      KeyLifecycleService.getInjector(0).resume();

      GenericTestUtils.waitFor(
          () -> log.getOutput().contains(KEY_COUNT + " expired keys and 0 expired dirs found and remained"),
          WAIT_CHECK_INTERVAL, 10000);

      OmKeyArgs key = keyList.get(ThreadLocalRandom.current().nextInt(keyList.size()));
      // update a key before before send deletion requests
      OzoneObj keyObj = new OzoneObjInfo.Builder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setKeyName(key.getKeyName())
          .setResType(OzoneObj.ResourceType.KEY)
          .setStoreType(OzoneObj.StoreType.OZONE)
          .build();
      OzoneAcl acl = OzoneAcl.of(IAccessAuthorizer.ACLIdentityType.USER, "user1",
          ACCESS, IAccessAuthorizer.ACLType.READ);
      writeClient.addAcl(keyObj, acl);
      LOG.info("key {} is updated to have a new ACL", key.getKeyName());

      Thread.sleep(SERVICE_INTERVAL);
      KeyLifecycleService.getInjector(1).resume();
      String expectedString = "Received a request to delete a Key /" + key.getVolumeName() + "/" +
          key.getBucketName() + "/" + key.getKeyName() + " whose updateID not match or null";
      GenericTestUtils.waitFor(() -> requestLog.getOutput().contains(expectedString), WAIT_CHECK_INTERVAL, 10000);

      // rename will change object's modificationTime. But since expiration action is an absolute timestamp, so
      // the renamed key will expire in next evaluation task
      GenericTestUtils.waitFor(() -> log.getOutput().contains("1 expired keys and 0 expired dirs found"),
          WAIT_CHECK_INTERVAL, 10000);
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == 0, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(KEY_COUNT, getDeletedKeyCount() - initialDeletedKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    /**
     * 100k keys.
     * Run 1
     * Processing Time (ms),Bucket Name,Iterated Keys,Deleted Keys,Deleted Data Size (bytes)
     * 2223,/testPerformanceWithExpiredKeys/bucket0,100000,100000,300000000
     * 1027,/testPerformanceWithExpiredKeys/bucket100001,100000,100000,300000000
     * 1044,/testPerformanceWithExpiredKeys/bucket200002,100000,100000,300000000
     * Run 2
     * Processing Time (ms),Bucket Name,Iterated Keys,Deleted Keys,Deleted Data Size (bytes)
     * 3751,/testPerformanceWithExpiredKeys/bucket0,100000,100000,300000000
     * 1137,/testPerformanceWithExpiredKeys/bucket100001,100000,100000,300000000
     * 1073,/testPerformanceWithExpiredKeys/bucket200002,100000,100000,300000000
     *
     * 500k keys
     * Run 1
     * Processing Time (ms),Bucket Name,Iterated Keys,Deleted Keys,Deleted Data Size (bytes)
     * 11136,/testPerformanceWithExpiredKeys/bucket0,500000,500000,1500000000
     * 5357,/testPerformanceWithExpiredKeys/bucket500001,500000,500000,1500000000
     * 5635,/testPerformanceWithExpiredKeys/bucket1000002,500000,500000,1500000000
     * Run 2
     * Processing Time (ms),Bucket Name,Iterated Keys,Deleted Keys,Deleted Data Size (bytes)
     * 14038,/testPerformanceWithExpiredKeys/bucket0,500000,500000,1500000000
     * 4475,/testPerformanceWithExpiredKeys/bucket500001,358289,358289,1074867000
     * 5259,/testPerformanceWithExpiredKeys/bucket1000002,500000,500000,1500000000
     */
    @ParameterizedTest
    @ValueSource(strings = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
    void testPerformanceWithExpiredKeys(BucketLayout bucketLayout)
        throws IOException, InterruptedException, TimeoutException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      long initialKeyDeleted = metrics.getNumKeyDeleted().value();
      long initialDirIterated = metrics.getNumDirIterated().value();
      long initialDirDeleted = metrics.getNumDirDeleted().value();
      final int keyCount = 10;
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, keyCount, 1, prefix, null);
      // check there are keys in keyTable
      assertEquals(keyCount, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == keyCount,
          WAIT_CHECK_INTERVAL, 1000);
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      List<OmLCRule> ruleList = new ArrayList<>();
      ruleList.add(new OmLCRule.Builder().setId(String.valueOf(OBJECT_ID_COUNTER.getAndIncrement()))
          .setEnabled(false).setPrefix(prefix)
          .setAction(new OmLCExpiration.Builder().setDate(date.toString()).build())
          .build());
      ruleList.add(new OmLCRule.Builder().setId(String.valueOf(OBJECT_ID_COUNTER.getAndIncrement()))
          .setEnabled(true).setPrefix(prefix)
          .setAction(new OmLCExpiration.Builder().setDate(date.toString()).build())
          .build());
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, ruleList);

      GenericTestUtils.waitFor(
          () -> (getDeletedKeyCount() - initialDeletedKeyCount) == keyCount, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);
      GenericTestUtils.waitFor(() -> metrics.getNumKeyDeleted().value() - initialKeyDeleted == keyCount,
          SERVICE_INTERVAL, 5000);
      assertEquals(0, metrics.getNumDirIterated().value() - initialDirIterated);
      assertEquals(0, metrics.getNumDirDeleted().value() - initialDirDeleted);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    public Stream<Arguments> parameters5() {
      return Stream.of(
          arguments(FILE_SYSTEM_OPTIMIZED, "dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/"),
          arguments(FILE_SYSTEM_OPTIMIZED,
              "dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/dir1/dir2/dir3/dir4/dir5/"),
          arguments(FILE_SYSTEM_OPTIMIZED,
              "dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/"),
          arguments(FILE_SYSTEM_OPTIMIZED,
              "dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/dir1/dir2/dir3/dir4/dir5/" +
                  "dir6/dir7/dir8/dir9/dir10/dir1/dir2/dir3/dir4/dir5/"),
          arguments(FILE_SYSTEM_OPTIMIZED,
              "dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/dir1/dir2/dir3/dir4/dir5/" +
                  "dir6/dir7/dir8/dir9/dir10/dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/"),
          arguments(BucketLayout.OBJECT_STORE, "dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/"),
          arguments(BucketLayout.OBJECT_STORE,
              "dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/dir1/dir2/dir3/dir4/dir5/"),
          arguments(BucketLayout.OBJECT_STORE,
              "dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/"),
          arguments(BucketLayout.OBJECT_STORE,
              "dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/dir1/dir2/dir3/dir4/dir5/" +
                  "dir6/dir7/dir8/dir9/dir10/dir1/dir2/dir3/dir4/dir5/"),
          arguments(BucketLayout.OBJECT_STORE,
              "dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/dir1/dir2/dir3/dir4/dir5/" +
                  "dir6/dir7/dir8/dir9/dir10/dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/")
      );
    }

    /**
     * ozone.om.ratis.log.appender.queue.byte-limit default is 32MB.
     * <p>
     * size 5900049 for 100000 keys like "dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/*" (40 bytes path)
     * size 8400049 for 100000 keys like
     * "dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/dir1/dir2/dir3/dir4/dir5/*" (60 bytes path)
     * size 11000049 for 100000 keys like
     * "dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/*"
     * (80 bytes path)
     * size 13600049 for 100000 keys like
     * "dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/dir1/dir2/dir3/dir4/dir5/" +
     * "dir6/dir7/dir8/dir9/dir10/dir1/dir2/dir3/dir4/dir5/" (100 bytes path)
     * size 16200049 for 100000 keys like
     * "dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/dir1/dir2/dir3/dir4/dir5/" +
     * "dir6/dir7/dir8/dir9/dir10/dir1/dir2/dir3/dir4/dir5/dir6/dir7/dir8/dir9/dir10/" (120 bytes path)
     */
    @ParameterizedTest
    @MethodSource("parameters5")
    void testPerformanceWithNestedDir(BucketLayout bucketLayout, String prefix)
        throws IOException, InterruptedException, TimeoutException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      long initialKeyDeleted = metrics.getNumKeyDeleted().value();
      long initialDirIterated = metrics.getNumDirIterated().value();
      long initialDirDeleted = metrics.getNumDirDeleted().value();
      final int keyCount = 20;
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, keyCount, 1, prefix, null);
      // check there are keys in keyTable
      Thread.sleep(SERVICE_INTERVAL);
      assertEquals(keyCount, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == keyCount,
          WAIT_CHECK_INTERVAL, 1000);
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      List<OmLCRule> ruleList = new ArrayList<>();
      ruleList.add(new OmLCRule.Builder().setId(String.valueOf(OBJECT_ID_COUNTER.getAndIncrement()))
          .setEnabled(false).setPrefix(prefix)
          .setAction(new OmLCExpiration.Builder().setDate(date.toString()).build())
          .build());
      ruleList.add(new OmLCRule.Builder().setId(String.valueOf(OBJECT_ID_COUNTER.getAndIncrement()))
          .setEnabled(true).setPrefix(prefix)
          .setAction(new OmLCExpiration.Builder().setDate(date.toString()).build())
          .build());
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, ruleList);

      GenericTestUtils.waitFor(
          () -> (getDeletedKeyCount() - initialDeletedKeyCount) == keyCount, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);
      GenericTestUtils.waitFor(() -> metrics.getNumKeyDeleted().value() - initialKeyDeleted == keyCount,
          WAIT_CHECK_INTERVAL, 5000);
      assertEquals(0, metrics.getNumDirIterated().value() - initialDirIterated);
      GenericTestUtils.waitFor(() ->
              metrics.getNumDirDeleted().value() - initialDirDeleted == (bucketLayout == FILE_SYSTEM_OPTIMIZED ? 1 : 0),
          WAIT_CHECK_INTERVAL, 10000);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    public Stream<Arguments> parameters6() {
      return Stream.of(
          arguments("FILE_SYSTEM_OPTIMIZED", true),
          arguments("FILE_SYSTEM_OPTIMIZED", false),
          arguments("LEGACY", true),
          arguments("LEGACY", false),
          arguments("OBJECT_STORE", true),
          arguments("OBJECT_STORE", false));
    }

    @ParameterizedTest
    @MethodSource("parameters6")
    void testListMaxSize(BucketLayout bucketLayout, boolean enableTrash) throws IOException,
        TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      long initialRenamedKeyCount = metrics.getNumKeyRenamed().value();
      final int keyCount = 100;
      final int maxListSize = 20;
      keyLifecycleService.setListMaxSize(maxListSize);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, keyCount, 1, prefix, null);
      // check there are keys in keyTable
      Thread.sleep(SERVICE_INTERVAL);
      assertEquals(keyCount, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == keyCount,
          WAIT_CHECK_INTERVAL, 1000);

      if (enableTrash) {
        final float trashInterval = 0.5f; // 30 seconds, 0.5 * (60 * 1000) ms
        conf.setFloat(FS_TRASH_INTERVAL_KEY, trashInterval);
        FileSystem fs = SecurityUtil.doAsLoginUser(
            (PrivilegedExceptionAction<FileSystem>)
                () -> new TrashOzoneFileSystem(om));
        keyLifecycleService.setOzoneTrash(new OzoneTrash(fs, conf, om));
      }

      GenericTestUtils.setLogLevel(KeyLifecycleService.getLog(), Level.DEBUG);
      GenericTestUtils.LogCapturer log =
          GenericTestUtils.LogCapturer.captureLogs(
              LoggerFactory.getLogger(KeyLifecycleService.class));
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, prefix, null, date.toString(), true);

      if (enableTrash && bucketLayout != OBJECT_STORE) {
        GenericTestUtils.waitFor(() ->
            (metrics.getNumKeyRenamed().value() - initialRenamedKeyCount) == keyCount, WAIT_CHECK_INTERVAL, 5000);
        assertEquals(0, getDeletedKeyCount() - initialDeletedKeyCount);
      } else {
        GenericTestUtils.waitFor(() ->
            (getDeletedKeyCount() - initialDeletedKeyCount) == keyCount, WAIT_CHECK_INTERVAL, 5000);
        assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);
      }
      GenericTestUtils.waitFor(() ->
          log.getOutput().contains("LimitedSizeList has reached maximum size " + maxListSize),
          WAIT_CHECK_INTERVAL, 5000);
      GenericTestUtils.setLogLevel(KeyLifecycleService.getLog(), Level.INFO);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    public Stream<Arguments> parameters7() {
      return Stream.of(
          arguments("FILE_SYSTEM_OPTIMIZED", "key"),
          arguments("FILE_SYSTEM_OPTIMIZED", "dir/key"),
          arguments("LEGACY", "key"),
          arguments("LEGACY", "dir/key"));
    }

    @ParameterizedTest
    @MethodSource("parameters7")
    void testMoveToTrash(BucketLayout bucketLayout, String prefix) throws IOException,
        TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      long initialRenamedKeyCount = metrics.getNumKeyRenamed().value();
      long initialRenamedDirCount = metrics.getNumDirRenamed().value();
      // create keys
      String bucketOwner = UserGroupInformation.getCurrentUser().getShortUserName() + "-test";
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, bucketOwner, KEY_COUNT, 1, prefix, null);
      // check there are keys in keyTable
      Thread.sleep(SERVICE_INTERVAL);
      assertEquals(KEY_COUNT, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);

      // enabled trash
      final float trashInterval = 0.5f; // 30 seconds, 0.5 * (60 * 1000) ms
      conf.setFloat(FS_TRASH_INTERVAL_KEY, trashInterval);
      FileSystem fs = SecurityUtil.doAsLoginUser(
          (PrivilegedExceptionAction<FileSystem>)
              () -> new TrashOzoneFileSystem(om));
      keyLifecycleService.setOzoneTrash(new OzoneTrash(fs, conf, om));

      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, "", null, date.toString(), true);

      GenericTestUtils.waitFor(() ->
          (metrics.getNumKeyRenamed().value() - initialRenamedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 50000);
      assertEquals(0, getDeletedKeyCount() - initialDeletedKeyCount);
      if (bucketLayout == FILE_SYSTEM_OPTIMIZED) {
        // Legacy bucket doesn't have dir concept
        GenericTestUtils.waitFor(() ->
                metrics.getNumDirRenamed().value() - initialRenamedDirCount == (prefix.contains(OM_KEY_PREFIX) ? 1 : 0),
            WAIT_CHECK_INTERVAL, 5000);
      }
      deleteLifecyclePolicy(volumeName, bucketName);
      // verify trash directory has the right native ACLs
      List<KeyInfoWithVolumeContext> dirList = new ArrayList<>();
      if (bucketLayout == FILE_SYSTEM_OPTIMIZED) {
        dirList.add(getDirectory(volumeName, bucketName, TRASH_PREFIX));
        dirList.add(getDirectory(volumeName, bucketName, TRASH_PREFIX + OM_KEY_PREFIX + bucketOwner));
        dirList.add(getDirectory(volumeName, bucketName, TRASH_PREFIX + OM_KEY_PREFIX + bucketOwner +
            OM_KEY_PREFIX + CURRENT));
      } else {
        dirList.add(getDirectory(volumeName, bucketName, TRASH_PREFIX + OM_KEY_PREFIX));
        dirList.add(getDirectory(volumeName, bucketName, TRASH_PREFIX + OM_KEY_PREFIX + bucketOwner + OM_KEY_PREFIX));
        dirList.add(getDirectory(volumeName, bucketName, TRASH_PREFIX + OM_KEY_PREFIX + bucketOwner +
            OM_KEY_PREFIX + CURRENT + OM_KEY_PREFIX));
      }
      for (KeyInfoWithVolumeContext dir : dirList) {
        List<OzoneAcl> aclList = dir.getKeyInfo().getAcls();
        for (OzoneAcl acl : aclList) {
          if (acl.getType() == IAccessAuthorizer.ACLIdentityType.USER ||
              acl.getType() == IAccessAuthorizer.ACLIdentityType.GROUP) {
            assertEquals(bucketOwner, acl.getName());
            assertTrue(acl.getAclList().contains(ALL));
          }
        }
      }

      GenericTestUtils.LogCapturer log =
          GenericTestUtils.LogCapturer.captureLogs(
              LoggerFactory.getLogger(KeyLifecycleService.class));

      // keys under trash directory is counted in getKeyCount()
      if (bucketLayout == FILE_SYSTEM_OPTIMIZED) {
        assertEquals(KEY_COUNT, getKeyCount(bucketLayout) - initialKeyCount);
      } else {
        // For legacy bucket, trash directories along .Trash/user-test/Current are in key table too.
        assertEquals(KEY_COUNT + (prefix.contains(OM_KEY_PREFIX) ? 4 : 3), getKeyCount(bucketLayout) - initialKeyCount);
      }
      // create new policy to test rule with prefix ".Trash/" is ignored during lifecycle evaluation
      now = ZonedDateTime.now(ZoneOffset.UTC);
      date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, TRASH_PREFIX + OM_KEY_PREFIX,
          null, date.toString(), true);

      GenericTestUtils.waitFor(
          () -> log.getOutput().contains("Skip rule") &&
              log.getOutput().contains("as its prefix starts with " + TRASH_PREFIX + OM_KEY_PREFIX),
          WAIT_CHECK_INTERVAL, 5000);
      deleteLifecyclePolicy(volumeName, bucketName);

      // create new policy to test rule with prefix ".Trash" is ignored during lifecycle evaluation
      now = ZonedDateTime.now(ZoneOffset.UTC);
      date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, TRASH_PREFIX, null, date.toString(), true);

      GenericTestUtils.waitFor(
          () -> log.getOutput().contains("Skip evaluate trash directory " + TRASH_PREFIX), WAIT_CHECK_INTERVAL, 5000);
      deleteLifecyclePolicy(volumeName, bucketName);

      // create new policy to test rule with prefix ".Tras" is ignored during lifecycle evaluation
      now = ZonedDateTime.now(ZoneOffset.UTC);
      date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, FILE_SYSTEM_OPTIMIZED, ".Tras", null, date.toString(), true);

      GenericTestUtils.waitFor(
          () -> log.getOutput().contains("Skip evaluate trash directory " + TRASH_PREFIX), WAIT_CHECK_INTERVAL, 5000);
      deleteLifecyclePolicy(volumeName, bucketName);

      // create new policy to test trash directory is skipped during lifecycle evaluation
      now = ZonedDateTime.now(ZoneOffset.UTC);
      date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, "", null, date.toString(), true);

      GenericTestUtils.waitFor(
          () -> log.getOutput().contains("No expired keys/dirs found/remained for bucket"), WAIT_CHECK_INTERVAL, 5000);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    public Stream<Arguments> parameters8() {
      return Stream.of(
          arguments("dir1/dir2/dir3/key", null, "dir1/dir", "dir1/dir2/dir3", KEY_COUNT, 2, false),
          arguments("dir1/dir2/dir3/key", null, "dir1/dir", "dir1/dir2/dir3", KEY_COUNT, 0, true),
          arguments("dir1/dir2/dir3/key", "dir1/dir4/dir5/key", "dir1/dir", "dir1/dir2/dir3", KEY_COUNT * 2, 4, false),
          arguments("dir1/dir2/dir3/key", "dir1/dir4/dir5/key", "dir1/dir", "dir1/dir2/dir3", KEY_COUNT * 2, 2, true),
          arguments("dir1/dir2/dir3/key", "dir1/dir22/dir5/key", "dir1/dir2/", "dir1/dir2/dir3", KEY_COUNT, 2, false),
          arguments("dir1/dir2/dir3/key", "dir1/dir22/dir5/key", "dir1/dir2/", "dir1/dir2/dir3", KEY_COUNT, 0, true),
          arguments("dir1/dir2/dir3/key", "dir1/dir22/dir5/key", "dir1/dir2", "dir1/dir2/dir3",
              KEY_COUNT * 2, 4, false),
          arguments("dir1/dir2/dir3/key", "dir1/dir22/dir5/key", "dir1/dir2", "dir1/dir2/dir3", KEY_COUNT * 2, 2, true),
          arguments("dir1/dir2/dir3/key", "dir1/dir4/dir5/key", "dir", "dir1/dir2/dir3", KEY_COUNT * 2, 5, false),
          arguments("dir1/dir2/dir3/key", "dir1/dir4/dir5/key", "dir", "dir1/dir2/dir3", KEY_COUNT * 2, 2, true),
          arguments("dir1/dir2/dir3/key", "dir11/dir4/dir5/key", "dir1/", "dir1/dir2/dir3", KEY_COUNT, 3, false),
          arguments("dir1/dir2/dir3/key", "dir11/dir4/dir5/key", "dir1/", "dir1/dir2/dir3", KEY_COUNT, 0, true),
          arguments("dir1/dir2/dir3/key", "dir11/dir4/dir5/key", "dir1", "dir1/dir2/dir3", KEY_COUNT * 2, 6, false),
          arguments("dir1/dir2/dir3/key", "dir11/dir4/dir5/key", "dir1", "dir1/dir2/dir3", KEY_COUNT * 2, 3, true),
          arguments("dir1/dir2/dir3/key", "dir1/dir4/dir5/key", "", "dir1/dir2/dir3", KEY_COUNT * 2, 5, false),
          arguments("dir1/dir2/dir3/key", "dir1/dir4/dir5/key", "", "dir1/dir2/dir3", KEY_COUNT * 2, 2, true));
    }

    @ParameterizedTest
    @MethodSource("parameters8")
    void testMultipleDirectoriesMatched(String keyPrefix1, String keyPrefix2, String rulePrefix, String dirName,
        int expectedDeletedKeyCount, int expectedDeletedDirCount, boolean updateDirModificationTime)
        throws IOException, TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialDeletedDirCount = getDeletedDirectoryCount();
      long initialKeyCount = getKeyCount(FILE_SYSTEM_OPTIMIZED);
      // create keys
      List<OmKeyArgs> keyList1 =
          createKeys(volumeName, bucketName, FILE_SYSTEM_OPTIMIZED, KEY_COUNT, 1, keyPrefix1, null);
      // check there are keys in keyTable
      assertEquals(KEY_COUNT, keyList1.size());
      GenericTestUtils.waitFor(() -> getKeyCount(FILE_SYSTEM_OPTIMIZED) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);

      if (keyPrefix2 != null) {
        List<OmKeyArgs> keyList2 = new ArrayList<>();
        for (int x = 0; x < KEY_COUNT; x++) {
          final String keyName = uniqueObjectName(keyPrefix2);
          OmKeyArgs keyArg = createAndCommitKey(volumeName, bucketName, keyName, 1, null);
          keyList2.add(keyArg);
        }
        // check there are keys in keyTable
        assertEquals(KEY_COUNT, keyList2.size());
        GenericTestUtils.waitFor(() -> getKeyCount(FILE_SYSTEM_OPTIMIZED) - initialKeyCount == KEY_COUNT * 2,
            WAIT_CHECK_INTERVAL, 1000);
      }

      // assert directory exists
      KeyInfoWithVolumeContext keyInfo = getDirectory(volumeName, bucketName, dirName);
      assertFalse(keyInfo.getKeyInfo().isFile());

      KeyLifecycleService.setInjectors(
          Arrays.asList(new FaultInjectorImpl(), new FaultInjectorImpl()));

      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, FILE_SYSTEM_OPTIMIZED, rulePrefix, null, date.toString(), true);
      LOG.info("expiry date {}", date.toInstant());

      ZonedDateTime endDate = date.plus(SERVICE_INTERVAL, ChronoUnit.MILLIS);
      GenericTestUtils.waitFor(() -> endDate.isBefore(ZonedDateTime.now(ZoneOffset.UTC)), WAIT_CHECK_INTERVAL, 10000);

      // rename a key under directory to change directory's Modification time
      if (updateDirModificationTime) {
        writeClient.renameKey(keyList1.get(0), keyList1.get(0).getKeyName() + "-new");
        LOG.info("Dir {} refreshes its modification time", dirName);
        KeyInfoWithVolumeContext keyInfo2 = getDirectory(volumeName, bucketName, dirName);
        assertNotEquals(keyInfo.getKeyInfo().getModificationTime(), keyInfo2.getKeyInfo().getModificationTime());
      }

      // resume KeyLifecycleService bucket scan
      KeyLifecycleService.getInjector(0).resume();
      KeyLifecycleService.getInjector(1).resume();

      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) == expectedDeletedKeyCount, WAIT_CHECK_INTERVAL, 10000);
      if (keyPrefix2 == null) {
        assertEquals(0, getKeyCount(FILE_SYSTEM_OPTIMIZED) - initialKeyCount);
      } else {
        assertEquals(KEY_COUNT * 2 - expectedDeletedKeyCount, getKeyCount(FILE_SYSTEM_OPTIMIZED) - initialKeyCount);
      }
      GenericTestUtils.waitFor(() -> getDeletedDirectoryCount() - initialDeletedDirCount == expectedDeletedDirCount,
          WAIT_CHECK_INTERVAL, 10000);
      if (updateDirModificationTime) {
        KeyInfoWithVolumeContext directory = getDirectory(volumeName, bucketName, dirName);
        assertNotNull(directory);
      } else {
        assertThrows(OMException.class, () -> getDirectory(volumeName, bucketName, dirName));
      }
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @Test
    void testGetLifecycleServiceStatus() throws Exception {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "key";
      
      //Service should be enabled but not running
      OzoneManagerProtocolProtos.GetLifecycleServiceStatusResponse status =
          om.getLifecycleServiceStatus();
      assertTrue(status.getIsEnabled());
      assertEquals(0, status.getRunningBucketsCount());
      
      // Create and inject for test
      createKeys(volumeName, bucketName, FILE_SYSTEM_OPTIMIZED, KEY_COUNT, 1, prefix, null);
      ZonedDateTime date = ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(EXPIRE_SECONDS);
      KeyLifecycleService.setInjectors(
          Arrays.asList(new FaultInjectorImpl(), new FaultInjectorImpl()));
      createLifecyclePolicy(volumeName, bucketName, FILE_SYSTEM_OPTIMIZED, prefix, null, date.toString(), true);
      Thread.sleep(SERVICE_INTERVAL + 100);
      
      // Verify service is running and processing the bucket
      status = om.getLifecycleServiceStatus();
      assertTrue(status.getIsEnabled());
      assertEquals(1, status.getRunningBucketsCount());
      assertTrue(status.getRunningBucketsList().contains("/" + volumeName + "/" + bucketName));
      
      KeyLifecycleService.getInjector(0).resume();
      KeyLifecycleService.getInjector(1).resume();
      GenericTestUtils.waitFor(() -> om.getLifecycleServiceStatus().getRunningBucketsCount() == 0,
          WAIT_CHECK_INTERVAL, 10000);
      
      // Verify service completed and is no longer running
      status = om.getLifecycleServiceStatus();
      assertTrue(status.getIsEnabled());
      assertEquals(0, status.getRunningBucketsCount());
      
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @Test
    void testDisableMoveToTrashDeletesDirectly() throws Exception {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      final String prefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(FILE_SYSTEM_OPTIMIZED);
      long initialRenamedKeyCount = metrics.getNumKeyRenamed().value();

      // Create keys
      createKeys(volumeName, bucketName, FILE_SYSTEM_OPTIMIZED, KEY_COUNT, 1, prefix, null);
      Thread.sleep(SERVICE_INTERVAL);
      GenericTestUtils.waitFor(() -> getKeyCount(FILE_SYSTEM_OPTIMIZED) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);

      // Make trash available, but disable move-to-trash in KeyLifecycleService.
      keyLifecycleService.setMoveToTrashEnabled(false);
      final float trashInterval = 0.5f; // 30 seconds
      conf.setFloat(FS_TRASH_INTERVAL_KEY, trashInterval);
      FileSystem fs = SecurityUtil.doAsLoginUser(
          (PrivilegedExceptionAction<FileSystem>)
              () -> new TrashOzoneFileSystem(om));
      keyLifecycleService.setOzoneTrash(new OzoneTrash(fs, conf, om));

      // Expire keys
      ZonedDateTime date = ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, FILE_SYSTEM_OPTIMIZED, "", null, date.toString(), true);

      // With move-to-trash disabled, keys should be deleted directly (not renamed).
      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(initialRenamedKeyCount, metrics.getNumKeyRenamed().value());
      assertEquals(0, getKeyCount(FILE_SYSTEM_OPTIMIZED) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }
  }

  /**
   * Tests failure scenarios.
   */
  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class Failing {

    @BeforeAll
    void setup(@TempDir File testDir) throws Exception {
      // failCallsFrequency = 1 means all calls fail
      scmBlockTestingClient = new ScmBlockLocationTestingClient(null, null, 1);
      createConfig(testDir);
      createSubject();
      keyDeletingService.suspend();
      directoryDeletingService.suspend();
    }

    @AfterEach
    void resume() {
    }

    @AfterAll
    void cleanup() {
      if (om.stop()) {
        om.join();
      }
    }

    /**
     * These are the operations supported for bucket and volume currently.
     * Bucket operation
     * a. can change owner
     * b. cannot be renamed
     * c. key from one bucket cannot be renamed to another bucket
     * d. can be deleted through sh
     * <p>
     * Volume
     * a. cannot be renamed
     * b. can be deleted through sh
     * c. can change owner
     * <p>
     * So overall, change owner and delete are allowed for bucket and volume. Since the service runs in background,
     * owner change doesn't have impact, only the bucket deletion.
     */
    @ParameterizedTest
    @ValueSource(strings = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
    void testBucketDeleted(BucketLayout bucketLayout) throws IOException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      long initialDeletedKeyCount = getDeletedKeyCount();
      GenericTestUtils.LogCapturer log =
          GenericTestUtils.LogCapturer.captureLogs(
              LoggerFactory.getLogger(KeyLifecycleService.class));
      createVolumeAndBucket(volumeName, bucketName, bucketLayout,
          UserGroupInformation.getCurrentUser().getShortUserName());
      assertNotNull(writeClient.getBucketInfo(volumeName, bucketName));

      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);

      FaultInjectorImpl injector = new FaultInjectorImpl();
      KeyLifecycleService.setInjectors(Arrays.asList(injector));
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, "", null, date.toString(), true);

      Thread.sleep(1000);
      writeClient.deleteBucket(volumeName, bucketName);
      assertThrows(OMException.class, () -> writeClient.getBucketInfo(volumeName, bucketName));
      injector.resume();

      Thread.sleep(SERVICE_INTERVAL);
      assertEquals(initialDeletedKeyCount, getDeletedKeyCount());
      String logString = log.getOutput();
      deleteLifecyclePolicy(volumeName, bucketName);
      String expectedString = "Bucket " + "/" + volumeName + "/" + bucketName + " cannot be found, " +
          "might be deleted during this task's execution";
      assertTrue(logString.contains(expectedString));
      log.clearOutput();
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    public Stream<Arguments> parameters1() {
      return Stream.of(
          arguments(FILE_SYSTEM_OPTIMIZED, true),
          arguments(FILE_SYSTEM_OPTIMIZED, false),
          arguments(BucketLayout.OBJECT_STORE, true),
          arguments(BucketLayout.OBJECT_STORE, false)
      );
    }

    @ParameterizedTest
    @MethodSource("parameters1")
    void testKeyDeletedOrRenamed(BucketLayout bucketLayout, boolean deleted)
        throws IOException, InterruptedException, TimeoutException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      GenericTestUtils.LogCapturer log =
          GenericTestUtils.LogCapturer.captureLogs(
              LoggerFactory.getLogger(KeyLifecycleService.class));
      GenericTestUtils.LogCapturer requestLog =
          GenericTestUtils.LogCapturer.captureLogs(
              LoggerFactory.getLogger(OMKeysDeleteRequest.class));
      String prefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, prefix, null);

      KeyLifecycleService.setInjectors(
          Arrays.asList(new FaultInjectorImpl(), new FaultInjectorImpl()));

      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, prefix, null, date.toString(), true);
      Thread.sleep(SERVICE_INTERVAL);
      KeyLifecycleService.getInjector(0).resume();

      GenericTestUtils.waitFor(
          () -> log.getOutput().contains(KEY_COUNT + " expired keys and 0 expired dirs found"),
          WAIT_CHECK_INTERVAL, 10000);
      OmKeyArgs key = keyList.get(ThreadLocalRandom.current().nextInt(1, keyList.size()));
      // delete/rename another key before send deletion requests
      if (deleted) {
        writeClient.deleteKey(key);
      } else {
        writeClient.renameKey(key, key.getKeyName() + System.currentTimeMillis());
      }
      LOG.info("key {} is deleted or renamed", key.getKeyName());

      Thread.sleep(SERVICE_INTERVAL);
      KeyLifecycleService.getInjector(1).resume();
      String expectedString = "Received a request to delete a Key does not exist /" + key.getVolumeName() + "/" +
          key.getBucketName() + "/" + key.getKeyName();
      GenericTestUtils.waitFor(() -> requestLog.getOutput().contains(expectedString), WAIT_CHECK_INTERVAL, 10000);
      if (!deleted) {
        // Since expiration action is an absolute timestamp, so the renamed key will expire in next evaluation task
        GenericTestUtils.waitFor(() -> log.getOutput().contains("1 expired keys and 0 expired dirs found"),
            SERVICE_INTERVAL, 10000);
      }
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == 0, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(KEY_COUNT, getDeletedKeyCount() - initialDeletedKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    public Stream<Arguments> parameters2() {
      return Stream.of(
          arguments("/", true),
          arguments("/", false),
          arguments("//", true),
          arguments("//", false),
          arguments("//dir1", true),
          arguments("//dir1", false),
          arguments("dir1//", true),
          arguments("dir1//", false),
          arguments("dir1/.", true),
          arguments("dir1/.", false),
          arguments("dir1/.//", true),
          arguments("dir1/.//", false),
          arguments("dir1/..", true),
          arguments("dir1/..", false),
          arguments("dir1/..//", true),
          arguments("dir1/..//", false),
          arguments(":/dir1", true),
          arguments(":/dir1", false)
      );
    }

    @ParameterizedTest
    @MethodSource("parameters2")
    void testUnsupportedPrefixForFSO(String prefix, boolean createPrefix) {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");

      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      OMException omException;
      if (createPrefix) {
        omException = assertThrows(
            OMException.class,
            () -> createLifecyclePolicy(volumeName, bucketName, FILE_SYSTEM_OPTIMIZED,
                prefix, null, date.toString(), true));
      } else {
        OmLCFilter.Builder filter = getOmLCFilterBuilder(prefix, null, null);
        omException = assertThrows(
            OMException.class,
            () -> createLifecyclePolicy(volumeName, bucketName, FILE_SYSTEM_OPTIMIZED,
                null, filter.build(), date.toString(), true));
      }
      assertSame(INVALID_REQUEST, omException.getResult());
    }
  }

  private List<OmKeyArgs> createKeys(String volume, String bucket, BucketLayout bucketLayout,
      int keyCount, int numBlocks, String keyPrefix, Map<String, String> tags) throws IOException {
    return createKeys(volume, bucket, bucketLayout, UserGroupInformation.getCurrentUser().getShortUserName(),
        keyCount, numBlocks, keyPrefix, tags);
  }

  @SuppressWarnings("parameternumber")
  private List<OmKeyArgs> createKeys(String volume, String bucket, BucketLayout bucketLayout, String owner,
      int keyCount, int numBlocks, String keyPrefix, Map<String, String> tags) throws IOException {
    // Create Volume and Bucket
    createVolumeAndBucket(volume, bucket, bucketLayout, owner);
    List<OmKeyArgs> keyList = new ArrayList<>();
    for (int x = 0; x < keyCount; x++) {
      final String keyName = uniqueObjectName(keyPrefix);
      // Create the key
      OmKeyArgs keyArg = createAndCommitKey(volume, bucket,
          keyName, numBlocks, tags);
      keyList.add(keyArg);
    }
    return keyList;
  }

  private OmLCFilter.Builder getOmLCFilterBuilder(String filterPrefix, Pair<String, String> filterTag,
      OmLifecycleRuleAndOperator andOperator) {
    OmLCFilter.Builder lcfBuilder = new OmLCFilter.Builder()
        .setPrefix(filterPrefix)
        .setAndOperator(andOperator);
    if (filterTag != null) {
      lcfBuilder.setTag(filterTag.getKey(), filterTag.getValue());
    }
    return lcfBuilder;
  }

  private OmLifecycleRuleAndOperator.Builder getOmLCAndOperatorBuilder(
      String prefix, Map<String, String> tags) {
    return new OmLifecycleRuleAndOperator.Builder()
        .setPrefix(prefix)
        .setTags(tags);
  }

  private void createLifecyclePolicy(String volume, String bucket, BucketLayout layout, String prefix,
      OmLCFilter filter, String date, boolean enabled) throws IOException {
    OmLifecycleConfiguration lcc = new OmLifecycleConfiguration.Builder()
        .setVolume(volume)
        .setBucket(bucket)
        .setBucketLayout(layout)
        .setBucketObjectID(bucketObjectID)
        .setRules(Collections.singletonList(new OmLCRule.Builder()
            .setId(String.valueOf(OBJECT_ID_COUNTER.getAndIncrement()))
            .setEnabled(enabled)
            .setPrefix(prefix)
            .setFilter(filter)
            .setAction(new OmLCExpiration.Builder()
                .setDate(date)
                .build())
            .build()))
        .build();
    String key = "/" + volume + "/" + bucket;
    LifecycleConfiguration lcProto = lcc.getProtobuf();
    OmLifecycleConfiguration canonicalLcc = OmLifecycleConfiguration.getFromProtobuf(lcProto);
    canonicalLcc.valid();
    metadataManager.getLifecycleConfigurationTable().put(key, lcc);
    metadataManager.getLifecycleConfigurationTable().addCacheEntry(
        new CacheKey<>(key), CacheValue.get(1L, canonicalLcc));
  }

  private void createLifecyclePolicy(String volume, String bucket, BucketLayout layout, List<OmLCRule> ruleList)
      throws IOException {
    OmLifecycleConfiguration lcc = new OmLifecycleConfiguration.Builder()
        .setVolume(volume)
        .setBucket(bucket)
        .setBucketObjectID(bucketObjectID)
        .setBucketLayout(layout)
        .setRules(ruleList)
        .build();
    String key = "/" + volume + "/" + bucket;
    LifecycleConfiguration lcProto = lcc.getProtobuf();
    OmLifecycleConfiguration canonicalLcc = OmLifecycleConfiguration.getFromProtobuf(lcProto);
    metadataManager.getLifecycleConfigurationTable().put(key, lcc);
    metadataManager.getLifecycleConfigurationTable().addCacheEntry(
        new CacheKey<>(key), CacheValue.get(1L, canonicalLcc));
  }

  private void deleteLifecyclePolicy(String volume, String bucket)
      throws IOException {
    String key = "/" + volume + "/" + bucket;
    metadataManager.getLifecycleConfigurationTable().delete(key);
    metadataManager.getLifecycleConfigurationTable().addCacheEntry(
        new CacheKey<>(key), CacheValue.get(1L));
  }

  private void createVolumeAndBucket(String volumeName,
      String bucketName, BucketLayout bucketLayout, String owner) throws IOException {
    // cheat here, just create a volume and bucket entry so that we can
    // create the keys, we put the same data for key and value since the
    // system does not decode the object
    OMRequestTestUtils.addVolumeToOM(keyManager.getMetadataManager(),
        OmVolumeArgs.newBuilder()
            .setOwnerName("o")
            .setAdminName("a")
            .setVolume(volumeName)
            .setObjectID(OBJECT_ID_COUNTER.incrementAndGet())
            .build());

    bucketObjectID = OBJECT_ID_COUNTER.incrementAndGet();
    OMRequestTestUtils.addBucketToOM(keyManager.getMetadataManager(),
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setBucketLayout(bucketLayout)
            .setOwner(owner)
            .setObjectID(bucketObjectID)
            .build());
  }

  private OmKeyArgs createAndCommitKey(String volumeName,
      String bucketName, String keyName, int numBlocks, Map<String, String> tags) throws IOException {
    return createAndCommitKey(volumeName, bucketName, keyName,
        numBlocks, 0, tags);
  }

  private OmKeyArgs createAndCommitKey(String volumeName,
      String bucketName, String keyName, int numBlocks, int numUncommitted, Map<String, String> tags)
      throws IOException {
    // Even if no key size is appointed, there will be at least one
    // block pre-allocated when key is created
    OmKeyArgs.Builder keyArgBuilder =
        new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setAcls(Collections.emptyList())
            .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
            .setDataSize(1000L)
            .setLocationInfoList(new ArrayList<>())
            .setOwnerName("user" + RandomStringUtils.randomNumeric(5))
            .setRecursive(true);

    if (tags != null) {
      for (Map.Entry<String, String> entry: tags.entrySet()) {
        keyArgBuilder.addTag(entry.getKey(), entry.getValue());
      }
    }
    //Open and Commit the Key in the Key Manager.
    OmKeyArgs keyArg = keyArgBuilder.build();
    OpenKeySession session = writeClient.openKey(keyArg);

    // add pre-allocated blocks into args and avoid creating excessive block
    OmKeyLocationInfoGroup keyLocationVersions = session.getKeyInfo().
        getLatestVersionLocations();
    assert keyLocationVersions != null;
    List<OmKeyLocationInfo> latestBlocks = keyLocationVersions.
        getBlocksLatestVersionOnly();
    int preAllocatedSize = latestBlocks.size();
    for (OmKeyLocationInfo block : latestBlocks) {
      keyArg.addLocationInfo(block);
    }

    // allocate blocks until the blocks num equal to numBlocks
    LinkedList<OmKeyLocationInfo> allocated = new LinkedList<>();
    for (int i = 0; i < numBlocks - preAllocatedSize; i++) {
      allocated.add(writeClient.allocateBlock(keyArg, session.getId(),
          new ExcludeList()));
    }

    // remove the blocks not to be committed
    for (int i = 0; i < numUncommitted; i++) {
      allocated.removeFirst();
    }

    // add the blocks to be committed
    for (OmKeyLocationInfo block: allocated) {
      keyArg.addLocationInfo(block);
    }

    writeClient.commitKey(keyArg, session.getId());
    return keyArg;
  }

  private KeyInfoWithVolumeContext getDirectory(String volumeName, String bucketName, String dirName)
      throws IOException {
    OmKeyArgs.Builder keyArgBuilder =
        new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(dirName);
    OmKeyArgs keyArg = keyArgBuilder.build();
    return writeClient.getKeyInfo(keyArg, false);
  }

  private void createDirectory(String volumeName, String bucketName, String dirName) throws IOException {
    OmKeyArgs.Builder keyArgBuilder =
        new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(dirName)
            .setOwnerName("test");
    OmKeyArgs keyArg = keyArgBuilder.build();
    writeClient.createDirectory(keyArg);
  }

  private long getDeletedKeyCount() {
    final Table<String, RepeatedOmKeyInfo> table = metadataManager.getDeletedTable();
    try {
      return metadataManager.countRowsInTable(table);
    } catch (IOException e) {
      fail("Failed to count deleted keys " + e.getMessage());
      return -1;
    }
  }

  private long getDeletedDirectoryCount() {
    final Table<String, OmKeyInfo> table = metadataManager.getDeletedDirTable();
    try {
      return metadataManager.countRowsInTable(table);
    } catch (IOException e) {
      fail("Failed to count deleted directories " + e.getMessage());
      return -1;
    }
  }

  private long getDirCount() throws IOException {
    final Table<String, OmDirectoryInfo> table = metadataManager.getDirectoryTable();
    return metadataManager.countRowsInTable(table);
  }

  private long getKeyCount(BucketLayout layout) {
    final Table<String, OmKeyInfo> table = metadataManager.getKeyTable(layout);
    try {
      return metadataManager.countRowsInTable(table);
    } catch (IOException e) {
      fail("Failed to count key" + e.getMessage());
      return -1;
    }
  }

  public static String uniqueObjectName(String prefix) {
    return prefix + OBJECT_COUNTER.getAndIncrement();
  }
}

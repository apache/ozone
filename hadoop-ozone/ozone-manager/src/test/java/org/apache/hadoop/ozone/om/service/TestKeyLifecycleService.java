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
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_INTERVAL_MS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_INTERVAL_MS_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_KEYS_PROCESSED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_KEYS_PROCESSED_DEFAULT;
import static org.apache.hadoop.ozone.om.OmConfig.Keys.ENABLE_FILESYSTEM_PATHS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.FILE_SYSTEM_OPTIMIZED;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.OBJECT_STORE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
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
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.FaultInjectorImpl;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
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
import org.apache.hadoop.ozone.om.helpers.OmLCAbortIncompleteMultipartUpload;
import org.apache.hadoop.ozone.om.helpers.OmLCExpiration;
import org.apache.hadoop.ozone.om.helpers.OmLCFilter;
import org.apache.hadoop.ozone.om.helpers.OmLCRule;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleConfiguration;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleRuleAndOperator;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleScanState;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
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
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
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
@ParameterizedClass
@MethodSource("stateSaveConfiguration")
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

  @Parameter(0)
  private long stateSaveInternal;

  @Parameter(1)
  private long maxKeysProcessedPerState;

  static Stream<Arguments> stateSaveConfiguration() {
    return Stream.of(
        Arguments.of(-1, -1),
        Arguments.of(OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_INTERVAL_MS_DEFAULT,
            OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_KEYS_PROCESSED_DEFAULT)
    );
  }

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
    conf.setLong(OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_INTERVAL_MS, stateSaveInternal);
    conf.setLong(OZONE_KEY_LIFECYCLE_SERVICE_STATE_SAVE_KEYS_PROCESSED, maxKeysProcessedPerState);
    OmLCExpiration.setTest(true);
    KeyLifecycleService.setTest(true);
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
      KeyLifecycleService.setInjectors(null);
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
      String keyPrefix = "key";
      String rulePrefix = bucketLayout == FILE_SYSTEM_OPTIMIZED ? "" : "key";
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
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, rulePrefix, null, date.toString(), true);
      } else {
        OmLCFilter.Builder filter = getOmLCFilterBuilder(rulePrefix, null, null);
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, null, filter.build(), date.toString(), true);
      }

      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @MethodSource("parameters1")
    void testScanStatePiggybackedOnDelete(BucketLayout bucketLayout, boolean createPrefix) throws IOException,
        TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String keyPrefix = "key";
      String rulePrefix = bucketLayout == FILE_SYSTEM_OPTIMIZED ? "" : "key";
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
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, rulePrefix, null, date.toString(), true);
      } else {
        OmLCFilter.Builder filter = getOmLCFilterBuilder(rulePrefix, null, null);
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, null, filter.build(), date.toString(), true);
      }

      // Wait for deletion
      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);

      // Verify that scan state was updated through the DeleteKeysRequest
      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      OmLifecycleScanState scanState = metadataManager.getLifecycleScanStateTable().get(bucketKey);
      assertNotNull(scanState);
      assertNotNull(scanState.getScanEndTime());

      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
    void testPeriodicStateSave(BucketLayout bucketLayout) throws Exception {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String keyPrefix = "key";
      String rulePrefix = bucketLayout == FILE_SYSTEM_OPTIMIZED ? "" : "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      long initialNumKeyDeleted = metrics.getNumKeyDeleted().value();
      long initialNumKeyIterated = metrics.getNumKeyIterated().value();
      int testKeyCount = 5;

      // Suspend service so it doesn't process immediately after we create the policy
      keyLifecycleService.suspend();

      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, testKeyCount, 1, keyPrefix, null);
      assertEquals(testKeyCount, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == testKeyCount,
          WAIT_CHECK_INTERVAL, 1000);

      // Inject spy to LifecycleScanStateTable to count put operations
      Field tableField = OmMetadataManagerImpl.class.getDeclaredField("lifecycleScanStateTable");
      tableField.setAccessible(true);
      Table<String, OmLifecycleScanState> originalTable =
          (Table<String, OmLifecycleScanState>) tableField.get(metadataManager);
      Table<String, OmLifecycleScanState> spyTable = spy(originalTable);
      tableField.set(metadataManager, spyTable);

      try {
        // create Lifecycle configuration
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, rulePrefix, null, date.toString(), true);

        // Resume the service
        keyLifecycleService.resume();

        // Wait for deletion
        GenericTestUtils.waitFor(() ->
            (getDeletedKeyCount() - initialDeletedKeyCount) == testKeyCount, WAIT_CHECK_INTERVAL, 10000);
        assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);

        String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);

        if (stateSaveInternal == -1) {
          // With 5 keys and stateSaveIntervalMs = -1, it should save on every key iteration.
          // It will save at least 5 times (one for each key).
          verify(spyTable, atLeast(5))
              .addCacheEntry(argThat(k -> k.getCacheKey().equals(bucketKey)), any());
        } else {
          // With 5 keys and maxKeysProcessedPerState = 100000, there is 1 save piggybacked in KeysDelete request.
          verify(spyTable, atLeast(1))
              .addCacheEntry(argThat(k -> k.getCacheKey().equals(bucketKey)), any());
        }
        GenericTestUtils.waitFor(() ->
            (getDeletedKeyCount() - initialDeletedKeyCount) == testKeyCount, WAIT_CHECK_INTERVAL, 5000);
        GenericTestUtils.waitFor(() ->
            (getDeletedKeyCount() - initialDeletedKeyCount) == testKeyCount, WAIT_CHECK_INTERVAL, 5000);
        GenericTestUtils.waitFor(() ->
            (metrics.getNumKeyDeleted().value() - initialNumKeyDeleted) == testKeyCount, WAIT_CHECK_INTERVAL, 5000);
        GenericTestUtils.waitFor(() ->
            (metrics.getNumKeyIterated().value() - initialNumKeyIterated) == testKeyCount, WAIT_CHECK_INTERVAL, 5000);
      } finally {
        deleteLifecyclePolicy(volumeName, bucketName);
      }
    }

    @ParameterizedTest
    @MethodSource("parameters1")
    void testBucketScanResume(BucketLayout bucketLayout, boolean createPrefix) throws IOException,
        TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String keyPrefix = "key";
      String rulePrefix = bucketLayout == FILE_SYSTEM_OPTIMIZED ? "" : "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      int testKeyCount = 5;

      // Suspend service so it doesn't process immediately after we create the policy
      keyLifecycleService.suspend();

      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, testKeyCount, 1, keyPrefix, null);
      assertEquals(testKeyCount, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == testKeyCount,
          WAIT_CHECK_INTERVAL, 1000);

      // determine db keys
      List<String> dbKeys = new ArrayList<>();
      long bucketId =
          metadataManager.getBucketTable().get(metadataManager.getBucketKey(volumeName, bucketName)).getObjectID();
      long volumeId = metadataManager.getVolumeTable().get(metadataManager.getVolumeKey(volumeName)).getObjectID();

      for (OmKeyArgs args : keyList) {
        if (bucketLayout == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
          dbKeys.add(metadataManager.getOzonePathKey(volumeId, bucketId, bucketId, args.getKeyName()));
        } else {
          dbKeys.add(metadataManager.getOzoneKey(volumeName, bucketName, args.getKeyName()));
        }
      }
      Collections.sort(dbKeys);
      String lastScannedDbKey = dbKeys.get(2); // The 3rd key

      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      if (createPrefix) {
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, rulePrefix, null, date.toString(), true);
      } else {
        OmLCFilter.Builder filter = getOmLCFilterBuilder(rulePrefix, null, null);
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, null, filter.build(), date.toString(), true);
      }

      // inject the resume state
      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);

      OmLifecycleConfiguration policy = metadataManager.getLifecycleConfiguration(volumeName, bucketName);
      OmLifecycleScanState.Builder stateBuilder = new OmLifecycleScanState.Builder()
          .setBucketKey(bucketKey)
          .setBucketObjID(bucketId)
          .setLifecycleConfigurationUpdateID(policy.getUpdateID())
          .setScanStartTime(System.currentTimeMillis());

      if (bucketLayout == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
        stateBuilder.setLastScannedKey(lastScannedDbKey);
        stateBuilder.setLastScannedDir("");
        stateBuilder.setLastScannedDirKey("");
      } else {
        stateBuilder.setLastScannedKey(lastScannedDbKey);
      }
      OmLifecycleScanState scanState = stateBuilder.build();
      metadataManager.getLifecycleScanStateTable().put(bucketKey, scanState);
      metadataManager.getLifecycleScanStateTable().addCacheEntry(new CacheKey<>(bucketKey),
          CacheValue.get(1L, scanState));
      OmLifecycleScanState state = metadataManager.getLifecycleScanStateTable().get(bucketKey);
      assertNotNull(state);

      // resume the service
      keyLifecycleService.resume();

      // wait for it to process
      // it should skip the first 3 keys (index 0, 1, 2) since we set lastScannedDbKey as index 2.
      // So it deletes only the last 2 keys (index 3 and 4).
      int expectedDeleted = 2;
      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) >= expectedDeleted, WAIT_CHECK_INTERVAL, 10000);
      
      // confirm it hasn't deleted all keys
      assertEquals(testKeyCount - expectedDeleted, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @MethodSource("parameters1")
    void testBucketScanWithScanEndTime(BucketLayout bucketLayout, boolean createPrefix) throws IOException,
        TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String keyPrefix = "key";
      String rulePrefix = bucketLayout == FILE_SYSTEM_OPTIMIZED ? "" : "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      int testKeyCount = 5;

      // Suspend service so it doesn't process immediately after we create the policy
      keyLifecycleService.suspend();

      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, testKeyCount, 1, keyPrefix, null);
      assertEquals(testKeyCount, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == testKeyCount,
          WAIT_CHECK_INTERVAL, 1000);

      // determine db keys
      List<String> dbKeys = new ArrayList<>();
      long bucketId =
          metadataManager.getBucketTable().get(metadataManager.getBucketKey(volumeName, bucketName)).getObjectID();
      long volumeId = metadataManager.getVolumeTable().get(metadataManager.getVolumeKey(volumeName)).getObjectID();

      for (OmKeyArgs args : keyList) {
        if (bucketLayout == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
          dbKeys.add(metadataManager.getOzonePathKey(volumeId, bucketId, bucketId, args.getKeyName()));
        } else {
          dbKeys.add(metadataManager.getOzoneKey(volumeName, bucketName, args.getKeyName()));
        }
      }
      Collections.sort(dbKeys);
      String lastScannedDbKey = dbKeys.get(2); // The 3rd key

      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      if (createPrefix) {
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, rulePrefix, null, date.toString(), true);
      } else {
        OmLCFilter.Builder filter = getOmLCFilterBuilder(rulePrefix, null, null);
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, null, filter.build(), date.toString(), true);
      }

      // inject the resume state but with ScanEndTime set!
      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      OmLifecycleConfiguration policy = metadataManager.getLifecycleConfiguration(volumeName, bucketName);
      OmLifecycleScanState.Builder stateBuilder = new OmLifecycleScanState.Builder()
          .setBucketKey(bucketKey)
          .setBucketObjID(bucketId)
          .setLifecycleConfigurationUpdateID(policy.getUpdateID())
          .setScanStartTime(System.currentTimeMillis())
          .setScanEndTime(System.currentTimeMillis()); // Scan finished previously

      if (bucketLayout == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
        stateBuilder.setLastScannedKey(lastScannedDbKey);
        stateBuilder.setLastScannedDir("");
      } else {
        stateBuilder.setLastScannedKey(lastScannedDbKey);
      }
      OmLifecycleScanState scanState = stateBuilder.build();
      metadataManager.getLifecycleScanStateTable().put(bucketKey, scanState);
      metadataManager.getLifecycleScanStateTable().addCacheEntry(new CacheKey<>(bucketKey),
          CacheValue.get(1L, scanState));

      // resume the service
      keyLifecycleService.resume();

      // wait for it to process
      // Since ScanEndTime is set, it will ignore the lastScannedKey and start from the beginning!
      // So it should delete ALL 5 keys.
      int expectedDeleted = 5;
      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) >= expectedDeleted, WAIT_CHECK_INTERVAL, 10000);
      
      // confirm it has deleted all keys
      assertEquals(testKeyCount - expectedDeleted, getKeyCount(bucketLayout) - initialKeyCount);

      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @MethodSource("parameters1")
    void testScanEmptyBucket(BucketLayout bucketLayout, boolean moveToTrash) throws Exception {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      
      keyLifecycleService.setMoveToTrashEnabled(moveToTrash);
      
      // Create empty bucket
      createVolumeAndBucket(volumeName, bucketName, bucketLayout,
          UserGroupInformation.getCurrentUser().getShortUserName());
          
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      OmLCFilter.Builder filter = bucketLayout == FILE_SYSTEM_OPTIMIZED ? getOmLCFilterBuilder("", null, null) :
          getOmLCFilterBuilder("key", null, null);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, null, filter.build(), date.toString(), true);

      // Wait until scan completes. Since the bucket is empty, it will scan immediately.
      // We check if the scanEndTime is set.
      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      
      GenericTestUtils.waitFor(() -> {
        try {
          OmLifecycleScanState scanState = metadataManager.getLifecycleScanStateTable().get(bucketKey);
          return scanState != null && scanState.getScanEndTime() != null;
        } catch (IOException e) {
          return false;
        }
      }, WAIT_CHECK_INTERVAL, 10000);

      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @MethodSource("parameters1")
    void testScanStateFailureDoesNotImpactScan(BucketLayout bucketLayout, boolean createPrefix) throws Exception {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String keyPrefix = "key";
      String rulePrefix = bucketLayout == FILE_SYSTEM_OPTIMIZED ? "" : "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, keyPrefix, null);
      assertEquals(KEY_COUNT, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == KEY_COUNT,
          WAIT_CHECK_INTERVAL, 1000);

      // Inject failure to LifecycleScanStateTable
      Field tableField = OmMetadataManagerImpl.class.getDeclaredField("lifecycleScanStateTable");
      tableField.setAccessible(true);
      Table<String, OmLifecycleScanState> originalTable =
          (Table<String, OmLifecycleScanState>) tableField.get(metadataManager);
      Table<String, OmLifecycleScanState> spyTable = spy(originalTable);
      doThrow(new RocksDatabaseException("Injected exception for testing")).when(spyTable).get(any());
      doThrow(new RocksDatabaseException("Injected exception for testing")).when(spyTable).put(any(), any());
      tableField.set(metadataManager, spyTable);

      try {
        // create Lifecycle configuration
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
        if (createPrefix) {
          createLifecyclePolicy(volumeName, bucketName, bucketLayout, rulePrefix, null, date.toString(), true);
        } else {
          OmLCFilter.Builder filter = getOmLCFilterBuilder(rulePrefix, null, null);
          createLifecyclePolicy(volumeName, bucketName, bucketLayout, null, filter.build(), date.toString(), true);
        }

        // Even though reading scan state fails, the deletion should still proceed normally.
        GenericTestUtils.waitFor(() ->
            (getDeletedKeyCount() - initialDeletedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 10000);
        assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);

        deleteLifecyclePolicy(volumeName, bucketName);
      } finally {
        // Restore original table
        tableField.set(metadataManager, originalTable);
      }
    }

    public Stream<Arguments> parameters12() {
      return Stream.of(
          arguments(FILE_SYSTEM_OPTIMIZED, 2),
          arguments(FILE_SYSTEM_OPTIMIZED, 3),
          arguments(FILE_SYSTEM_OPTIMIZED, 7),
          arguments(BucketLayout.OBJECT_STORE, 2),
          arguments(BucketLayout.OBJECT_STORE, 3),
          arguments(BucketLayout.OBJECT_STORE, 7)
      );
    }

    @ParameterizedTest
    @MethodSource("parameters12")
    void testNestedFSODirectoryScanResume(BucketLayout bucketLayout, int maxSize) throws Exception {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      long keyIterated = metrics.getNumKeyIterated().value();
      int testKeyCount = 8;

      keyLifecycleService.setListMaxSize(maxSize);
      // Suspend service so it doesn't process immediately after we create the policy
      keyLifecycleService.suspend();

      createVolumeAndBucket(volumeName, bucketName, bucketLayout,
          UserGroupInformation.getCurrentUser().getShortUserName());
      /**
       *  Create nested directory and 8 keys inside
       *               /
       *    dir1  dir2  dir3  dir30
       *    / \          / \
       *  dir4  dir5   dir6 dir7
       *               / \
       *             dir8 dir9
       *
       *  MaxSize = 2, lastScannedDir is dir9, lastScannedKey is key8
       *  MaxSize = 3, lastScannedDir is dir8, lastScannedKey is key6
       *  MaxSize = 7, lastScannedDir is dir5, lastScannedKey is key3
       */
      List<OmKeyArgs> keyList = new ArrayList<>();
      keyList.add(createAndCommitKey(volumeName, bucketName, "dir1/dir4/key0", 1, null));
      keyList.add(createAndCommitKey(volumeName, bucketName, "dir1/dir4/key1", 1, null));
      keyList.add(createAndCommitKey(volumeName, bucketName, "dir1/dir5/key2", 1, null));
      keyList.add(createAndCommitKey(volumeName, bucketName, "dir1/dir5/key3", 1, null));
      keyList.add(createAndCommitKey(volumeName, bucketName, "dir3/dir6/dir8/key5", 1, null));
      keyList.add(createAndCommitKey(volumeName, bucketName, "dir3/dir6/dir8/key6", 1, null));
      keyList.add(createAndCommitKey(volumeName, bucketName, "dir3/dir6/dir9/key7", 1, null));
      keyList.add(createAndCommitKey(volumeName, bucketName, "dir3/dir6/dir9/key8", 1, null));
      if (bucketLayout == FILE_SYSTEM_OPTIMIZED) {
        createDirectory(volumeName, bucketName, "dir2");
        createDirectory(volumeName, bucketName, "dir3/dir7");
        createDirectory(volumeName, bucketName, "dir30");
      }

      assertEquals(testKeyCount, keyList.size());
      GenericTestUtils.waitFor(
          () -> getKeyCount(bucketLayout) - initialKeyCount == testKeyCount, WAIT_CHECK_INTERVAL, 1000);

      // Create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, prefix, null, date.toString(), true);

      // Inject to cause resume case for FSO with nested directory
      FaultInjectorImpl lastFaultInjector = new FaultInjectorImpl();
      lastFaultInjector.setException(new IOException("Injected exception for testing"));
      KeyLifecycleService.setInjectors(
          Arrays.asList(new FaultInjectorImpl(), new FaultInjectorImpl(), lastFaultInjector));
      // Resume the service
      keyLifecycleService.resume();
      KeyLifecycleService.getInjector(0).resume();
      KeyLifecycleService.getInjector(1).resume();

      // wait for scanState to be updated
      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      GenericTestUtils.waitFor(() -> {
        try {
          OmLifecycleScanState scanState = metadataManager.getLifecycleScanStateTable().get(bucketKey);
          if (bucketLayout == FILE_SYSTEM_OPTIMIZED) {
            return scanState != null && scanState.getLastScannedDir() != null && scanState.getLastScannedKey() != null;
          } else {
            return scanState != null && scanState.getLastScannedKey() != null;
          }
        } catch (IOException e) {
          return false;
        }
      }, WAIT_CHECK_INTERVAL, 10000);

      OmLifecycleScanState scanState = metadataManager.getLifecycleScanStateTable().get(bucketKey);
      if (stateSaveInternal != -1) {
        if (maxSize == 2) {
          if (bucketLayout == FILE_SYSTEM_OPTIMIZED) {
            assertEquals("dir3/dir6/dir9", scanState.getLastScannedDir());
            assertTrue(scanState.getLastScannedKey().endsWith("key8"));
          } else {
            assertTrue(scanState.getLastScannedKey().endsWith("key1"));
          }
        } else if (maxSize == 3) {
          if (bucketLayout == FILE_SYSTEM_OPTIMIZED) {
            assertEquals("dir3/dir6/dir8", scanState.getLastScannedDir());
            assertTrue(scanState.getLastScannedKey().endsWith("key6"));
          } else {
            assertTrue(scanState.getLastScannedKey().endsWith("key2"));
          }
        } else {
          if (bucketLayout == FILE_SYSTEM_OPTIMIZED) {
            assertEquals("dir1/dir5", scanState.getLastScannedDir());
            assertTrue(scanState.getLastScannedKey().endsWith("key3"));
          } else {
            assertTrue(scanState.getLastScannedKey().endsWith("key7"));
          }
        }
      }

      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) == testKeyCount, WAIT_CHECK_INTERVAL, 10000);
      // Confirm all keys are deleted
      assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);
      // Confirm iterated key number
      GenericTestUtils.waitFor(() ->
          testKeyCount == metrics.getNumKeyIterated().value() - keyIterated, WAIT_CHECK_INTERVAL, 5000);

      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @MethodSource("parameters1")
    void testOneKeyExpired(BucketLayout bucketLayout, boolean createPrefix) throws IOException,
        TimeoutException, InterruptedException {
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
      int keyIndex = ThreadLocalRandom.current().nextInt(KEY_COUNT - 1);
      String rulePrefix = bucketLayout == FILE_SYSTEM_OPTIMIZED ? "" : keyList.get(keyIndex).getKeyName();
      if (createPrefix) {
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, rulePrefix, null, date.toString(), true);
      } else {
        OmLCFilter.Builder filter = getOmLCFilterBuilder(rulePrefix, null, null);
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, null, filter.build(), date.toString(), true);
      }

      int expectedDeleteCount = bucketLayout == FILE_SYSTEM_OPTIMIZED ? KEY_COUNT : 1;
      GenericTestUtils.waitFor(() -> (getDeletedKeyCount() - initialDeletedKeyCount) == expectedDeleteCount,
          WAIT_CHECK_INTERVAL, 5000);
      assertEquals(KEY_COUNT - expectedDeleteCount, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    public Stream<Arguments> parameters13() {
      return Stream.of(
          arguments("dirC", null, 3),
          arguments("dirC/dir3", null, 3),
          arguments("dirB", new String[]{"dirC"}, 2),
          arguments("dirB/dir2", new String[]{"dirC"}, 2),
          arguments("dirA", new String[]{"dirC", "dirB"}, 1),
          arguments("dirA/dir1", new String[]{"dirC", "dirB"}, 1)
      );
    }

    @ParameterizedTest
    @MethodSource("parameters13")
    void testDirectorySkippedAfterResume(String lastScannedDir, String[] skippedDir, int expectedDeleted)
        throws Exception {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(BucketLayout.FILE_SYSTEM_OPTIMIZED);
      keyLifecycleService.setListMaxSize(1);
      // Suspend service so it doesn't process immediately after we create the policy
      keyLifecycleService.suspend();

      createVolumeAndBucket(volumeName, bucketName, BucketLayout.FILE_SYSTEM_OPTIMIZED,
          UserGroupInformation.getCurrentUser().getShortUserName());

      // Create 3 directories: dirA/dir1, dirB/dir2, dirC/dir3
      // Inside each directory, create 1 keys.
      int testKeyCount = 3;
      List<OmKeyArgs> keyList = new ArrayList<>();

      int i = 0;
      for (String dir : Arrays.asList("dirA/dir1", "dirB/dir2", "dirC/dir3")) {
        String keyName = dir + "/key" + i++;
        keyList.add(createAndCommitKey(volumeName, bucketName, keyName, 1, null));
      }

      assertEquals(testKeyCount, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(BucketLayout.FILE_SYSTEM_OPTIMIZED) - initialKeyCount == testKeyCount,
          WAIT_CHECK_INTERVAL, 1000);

      // Determine DB keys for the files
      long bucketId =
          metadataManager.getBucketTable().get(metadataManager.getBucketKey(volumeName, bucketName)).getObjectID();
      long volumeId = metadataManager.getVolumeTable().get(metadataManager.getVolumeKey(volumeName)).getObjectID();

      // Find dirB/dir2's objectID and its table key
      String dirBKey = metadataManager.getOzonePathKey(volumeId, bucketId, bucketId, lastScannedDir);

      // Create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, BucketLayout.FILE_SYSTEM_OPTIMIZED, prefix,
          null, date.toString(), true);

      // Inject the resume state for FSO where lastScannedDir is dirB
      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      OmLifecycleConfiguration policy = metadataManager.getLifecycleConfiguration(volumeName, bucketName);
      OmLifecycleScanState.Builder stateBuilder = new OmLifecycleScanState.Builder()
          .setBucketKey(bucketKey)
          .setBucketObjID(bucketId)
          .setLifecycleConfigurationUpdateID(policy.getUpdateID())
          .setScanStartTime(System.currentTimeMillis())
          .setLastScannedDir(lastScannedDir)
          .setLastScannedDirKey(dirBKey);

      OmLifecycleScanState scanState = stateBuilder.build();
      metadataManager.getLifecycleScanStateTable().put(bucketKey, scanState);
      metadataManager.getLifecycleScanStateTable().addCacheEntry(new CacheKey<>(bucketKey),
          CacheValue.get(1L, scanState));

      GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer.captureLogs(
          LoggerFactory.getLogger(KeyLifecycleService.class));
      // Resume the service
      keyLifecycleService.resume();

      // Wait for it to process
      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) == expectedDeleted, WAIT_CHECK_INTERVAL, 10000);
      
      // Confirm it hasn't deleted dirA's keys
      assertEquals(testKeyCount - expectedDeleted, getKeyCount(BucketLayout.FILE_SYSTEM_OPTIMIZED) - initialKeyCount);
      if (skippedDir != null) {
        Arrays.stream(skippedDir).forEach(
            d -> assertTrue(logCapturer.getOutput().contains("Skip " + d)));
        logCapturer.clearOutput();
      }

      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testLastScannedKeySeek(boolean keyBelongToDir) throws Exception {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(BucketLayout.FILE_SYSTEM_OPTIMIZED);
      long keyIterated = metrics.getNumKeyIterated().value();

      keyLifecycleService.suspend();

      createVolumeAndBucket(volumeName, bucketName, BucketLayout.FILE_SYSTEM_OPTIMIZED,
          UserGroupInformation.getCurrentUser().getShortUserName());

      List<OmKeyArgs> keyList = new ArrayList<>();
      keyList.add(createAndCommitKey(volumeName, bucketName, "dir1/key1", 1, null));
      keyList.add(createAndCommitKey(volumeName, bucketName, "dir1/key2", 1, null));
      keyList.add(createAndCommitKey(volumeName, bucketName, "dir2/key3", 1, null));
      keyList.add(createAndCommitKey(volumeName, bucketName, "dir2/key4", 1, null));
      
      assertEquals(4, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(BucketLayout.FILE_SYSTEM_OPTIMIZED) - initialKeyCount == 4,
          WAIT_CHECK_INTERVAL, 1000);

      long bucketId =
          metadataManager.getBucketTable().get(metadataManager.getBucketKey(volumeName, bucketName)).getObjectID();
      long volumeId = metadataManager.getVolumeTable().get(metadataManager.getVolumeKey(volumeName)).getObjectID();

      String dir1Key = metadataManager.getOzonePathKey(volumeId, bucketId, bucketId, "dir1");
      long dir1Id = metadataManager.getDirectoryTable().get(
          metadataManager.getOzonePathKey(volumeId, bucketId, bucketId, "dir1")).getObjectID();
      long dir2Id = metadataManager.getDirectoryTable().get(
          metadataManager.getOzonePathKey(volumeId, bucketId, bucketId, "dir2")).getObjectID();

      // key2 is under dir1
      String key2DbKey = metadataManager.getOzonePathKey(volumeId, bucketId, dir1Id, "key2");
      // key3 is under dir2
      String key3DbKey = metadataManager.getOzonePathKey(volumeId, bucketId, dir2Id, "key3");

      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      OmLCFilter.Builder filter = getOmLCFilterBuilder(prefix, null, null);
      createLifecyclePolicy(volumeName, bucketName, BucketLayout.FILE_SYSTEM_OPTIMIZED, null,
          filter.build(), date.toString(), true);

      // Set lastScannedDir to dir1, but lastScannedKey to key3 (which is in dir2)
      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      OmLifecycleConfiguration policy = metadataManager.getLifecycleConfiguration(volumeName, bucketName);
      OmLifecycleScanState.Builder stateBuilder = new OmLifecycleScanState.Builder()
          .setBucketKey(bucketKey)
          .setBucketObjID(bucketId)
          .setLifecycleConfigurationUpdateID(policy.getUpdateID())
          .setScanStartTime(System.currentTimeMillis());

      if (keyBelongToDir) {
        stateBuilder.setLastScannedDir("dir1").setLastScannedDirKey(dir1Key).setLastScannedKey(key2DbKey);
      } else {
        stateBuilder.setLastScannedDir("dir1").setLastScannedDirKey(dir1Key).setLastScannedKey(key3DbKey);
      }

      OmLifecycleScanState scanState = stateBuilder.build();
      metadataManager.getLifecycleScanStateTable().put(bucketKey, scanState);
      metadataManager.getLifecycleScanStateTable().addCacheEntry(new CacheKey<>(bucketKey),
          CacheValue.get(1L, scanState));

      GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer.captureLogs(
          LoggerFactory.getLogger(KeyLifecycleService.class));
      keyLifecycleService.resume();

      // dir1 can be fully evaluated depending on whether lastScannedKey belong to it (no seek) or not
      // dir2 should be skipped dir2 > dir1
      int expectedDeleted = 2;
      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) >= expectedDeleted, WAIT_CHECK_INTERVAL, 5000);
      assertEquals(keyList.size() - expectedDeleted, getKeyCount(BucketLayout.FILE_SYSTEM_OPTIMIZED) - initialKeyCount);
      GenericTestUtils.waitFor(() ->
          expectedDeleted == metrics.getNumKeyIterated().value() - keyIterated, WAIT_CHECK_INTERVAL, 5000);
      if (keyBelongToDir) {
        assertTrue(logCapturer.getOutput().contains("Seek to key"));
      } else {
        assertFalse(logCapturer.getOutput().contains("Seek to key"));
      }
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @ParameterizedTest
    @ValueSource(strings = {"FILE_SYSTEM_OPTIMIZED", "OBJECT_STORE"})
    void testBucketRootScannedDirResume(BucketLayout layout) throws Exception {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String prefix = "";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(layout);
      long keyIterated = metrics.getNumKeyIterated().value();

      keyLifecycleService.suspend();

      createVolumeAndBucket(volumeName, bucketName, layout,
          UserGroupInformation.getCurrentUser().getShortUserName());

      List<OmKeyArgs> keyList = new ArrayList<>();
      keyList.add(createAndCommitKey(volumeName, bucketName, "key1", 1, null));
      keyList.add(createAndCommitKey(volumeName, bucketName, "key2", 1, null));
      keyList.add(createAndCommitKey(volumeName, bucketName, "key3", 1, null));
      
      assertEquals(3, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(layout) - initialKeyCount == 3,
          WAIT_CHECK_INTERVAL, 1000);

      long bucketId =
          metadataManager.getBucketTable().get(metadataManager.getBucketKey(volumeName, bucketName)).getObjectID();
      long volumeId = metadataManager.getVolumeTable().get(metadataManager.getVolumeKey(volumeName)).getObjectID();

      // key2 in bucket root
      String key2DbKey = layout == FILE_SYSTEM_OPTIMIZED ?
          metadataManager.getOzonePathKey(volumeId, bucketId, bucketId, "key2") :
          metadataManager.getOzoneKey(volumeName, bucketName, "key2");

      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      OmLCFilter.Builder filter = getOmLCFilterBuilder(prefix, null, null);
      createLifecyclePolicy(volumeName, bucketName, layout, null,
          filter.build(), date.toString(), true);

      // Set lastScannedDir to "" (bucket root) and lastScannedKey to key2
      String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      OmLifecycleConfiguration policy = metadataManager.getLifecycleConfiguration(volumeName, bucketName);
      OmLifecycleScanState.Builder stateBuilder = new OmLifecycleScanState.Builder()
          .setBucketKey(bucketKey)
          .setBucketObjID(bucketId)
          .setLifecycleConfigurationUpdateID(policy.getUpdateID())
          .setScanStartTime(System.currentTimeMillis())
          .setLastScannedDir("")
          .setLastScannedDirKey("")
          .setLastScannedKey(key2DbKey);

      OmLifecycleScanState scanState = stateBuilder.build();
      metadataManager.getLifecycleScanStateTable().put(bucketKey, scanState);
      metadataManager.getLifecycleScanStateTable().addCacheEntry(new CacheKey<>(bucketKey),
          CacheValue.get(1L, scanState));
          
      keyLifecycleService.resume();

      // It should seek to key2. key1 is skipped, key2 is skipped too.
      // So key1 and key2 are skipped, key3 is deleted.
      int expectedDeleted = 1;
      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) == expectedDeleted, WAIT_CHECK_INTERVAL, 10000);
      
      assertEquals(2, getKeyCount(layout) - initialKeyCount);
      GenericTestUtils.waitFor(() ->
          expectedDeleted == metrics.getNumKeyIterated().value() - keyIterated, WAIT_CHECK_INTERVAL, 5000);
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
      String rulePrefix = bucketLayout == FILE_SYSTEM_OPTIMIZED ? "" : keyArg.getKeyName();
      if (createPrefix) {
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, rulePrefix, null, date.toString(), true);
      } else {
        OmLCFilter.Builder filter = getOmLCFilterBuilder(rulePrefix, null, null);
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
      String rulePrefix = bucketLayout == FILE_SYSTEM_OPTIMIZED ? "" : keyPrefix;
      OmLifecycleRuleAndOperator andOperator = getOmLCAndOperatorBuilder(rulePrefix, tags).build();
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
      String keyPrefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      Map<String, String> tags = ImmutableMap.of("app", "spark", "user", "ozone");
      // create keys without tags
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, keyCount, 1, keyPrefix, null);
      // create one more key with tag
      final String keyName = uniqueObjectName(keyPrefix);
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
      String rulePrefix = bucketLayout == FILE_SYSTEM_OPTIMIZED ? "" : keyPrefix;
      OmLifecycleRuleAndOperator andOperator = getOmLCAndOperatorBuilder(rulePrefix, tags).build();
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
      // FSO bucket must end with "/". "/" is also invalid prefix for FSO.
      assumeTrue(bucketLayout == OBJECT_STORE);
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      String keyPrefix = "key";
      String rulePrefix = "/" + keyPrefix;
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
        createLifecyclePolicy(volumeName, bucketName, bucketLayout, rulePrefix, null, date.toString(), true);
      } else {
        OmLCFilter.Builder filter = getOmLCFilterBuilder(rulePrefix, null, null);
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
      // FSO bucket doesn't allow "//" in prefix.
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
      // FSO bucket doesn't allow "//" in prefix.
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
      // FSO bucket must end with "/". "/" is also invalid prefix for FSO.
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
      String keyPrefix = "dir1/dir2/dir3/key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      long initialNumDeletedKey = metrics.getNumKeyDeleted().value();
      long initialSizeDeletedKey = metrics.getSizeKeyDeleted().value();
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
      String rulePrefix = bucketLayout == FILE_SYSTEM_OPTIMIZED ? "dir1/dir2/dir3/" : keyPrefix;
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, rulePrefix, null, date.toString(), true);

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
      String filterPrefix = "dir1/dir2/dir4/";
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
      String keyPrefix = "dir1/dir2/dir3/key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, keyPrefix, null);
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
      String rulePrefix = bucketLayout == FILE_SYSTEM_OPTIMIZED ? "dir1/dir2/dir3/" : keyPrefix;
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, rulePrefix, null, date.toString(), true);

      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    public Stream<Arguments> parameters3() {
      return Stream.of(
          arguments("dir1/dir2/dir3/key", "dir1/dir2/dir3/", "dir1/dir2/dir3"),
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
          arguments("dir1/dir2/dir3//", "dir1/dir2/dir3/", 3, true, false),
          arguments("dir1/dir2/dir3//", "dir1/dir2/dir3/", 3, false, true),
          arguments("dir1/dir2//", "dir1/dir2/", 2, true, false),
          arguments("dir1/dir2//", "dir1/dir2/", 2, false, true),
          arguments("dir1//", "dir1/", 1, true, false),
          arguments("dir1//", "dir1/", 1, false, true)
      );
    }

    @ParameterizedTest
    @MethodSource("parameters4")
    void testPrefixDirectoryNotExpired(String dirName, String prefix, int dirDepth, boolean createPrefix,
        boolean createFilterPrefix) throws IOException, TimeoutException, InterruptedException {
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

      GenericTestUtils.LogCapturer log =
          GenericTestUtils.LogCapturer.captureLogs(
              LoggerFactory.getLogger(KeyLifecycleService.class));

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

      GenericTestUtils.waitFor(() -> log.getOutput().contains("Prefix directory " + prefix + " doesn't get expired"),
          WAIT_CHECK_INTERVAL, 10000);
      assertEquals(dirDepth, getDirCount() - initialDirCount);
      assertEquals(0, metrics.getNumDirDeleted().value() - initialNumDeletedDir);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @Test
    void testConsolidatedPrefixNotHappen() throws IOException, TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      long initialDeletedKeyCount = getDeletedKeyCount();
      String dir1 = "dir/dir1/dir2/";
      String dir2 = "log/log1/log2/";

      // Create the directories
      createVolumeAndBucket(volumeName, bucketName, FILE_SYSTEM_OPTIMIZED,
          UserGroupInformation.getCurrentUser().getShortUserName());
      createDirectory(volumeName, bucketName, dir1);
      createDirectory(volumeName, bucketName, dir2);
      KeyInfoWithVolumeContext keyInfo = getDirectory(volumeName, bucketName, dir1);
      assertFalse(keyInfo.getKeyInfo().isFile());
      keyInfo = getDirectory(volumeName, bucketName, dir2);
      assertFalse(keyInfo.getKeyInfo().isFile());
      List<OmKeyArgs> keyList = new ArrayList<>();
      keyList.add(createAndCommitKey(volumeName, bucketName, dir1 + "key1", 1, null));
      keyList.add(createAndCommitKey(volumeName, bucketName, dir2 + "key2", 1, null));

      Thread.sleep(SERVICE_INTERVAL);

      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      List<OmLCRule> ruleList = new ArrayList<>();
      String ruleID1 = String.valueOf(OBJECT_ID_COUNTER.getAndIncrement());
      String ruleID2 = String.valueOf(OBJECT_ID_COUNTER.getAndIncrement());
      ruleList.add(new OmLCRule.Builder().setId(ruleID1)
          .setEnabled(true).setPrefix(dir1)
          .setAction(new OmLCExpiration.Builder().setDate(date.toString()).build())
          .build());
      ruleList.add(new OmLCRule.Builder().setId(ruleID2)
          .setEnabled(true).setPrefix(dir2)
          .setAction(new OmLCExpiration.Builder().setDate(date.toString()).build())
          .build());
      createLifecyclePolicy(volumeName, bucketName, FILE_SYSTEM_OPTIMIZED, ruleList);

      GenericTestUtils.waitFor(() -> getDeletedKeyCount() - initialDeletedKeyCount == keyList.size(),
          WAIT_CHECK_INTERVAL, 5000);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    public Stream<Arguments> parameters41() {
      return Stream.of(
          arguments("dir/dir1/", "dir/dir1/dir2/", null, null, true,
              "Prefix directory dir/dir1/dir2/ doesn't get expired", 1, "dir/dir1/", null),
          arguments("dir/dir1/", "dir/dir2/", null, null, false,
              "Prefix directory dir/dir2/ doesn't get expired", 2, "dir/dir2/", null),
          arguments("dir1/dir2/", "log1/log2/", null, null, false,
              "Prefix directory dir1/dir2/ doesn't get expired", 2, "log1/log2/", null),
          arguments("dir/dir1/", "dir/dir1/dir2/", "dir/", null, true,
              "Prefix directory dir/dir1/dir2/ doesn't get expired", 1, "dir/", null),
          arguments("dir/dir1/dir2/", "dir/", "dir/dir1/", null, true,
              "Prefix directory dir/dir1/dir2/ doesn't get expired", 1, "dir/", null),
          arguments("dir/", "dir/dir1/", "dir/dir1/dir2/", null, true,
              "Prefix directory dir/dir1/dir2/ doesn't get expired", 1, "dir/", null),
          arguments("dir/dir1/", "log/log1/", "dir/dir1/dir2/", null, true,
              "Prefix directory dir/dir1/dir2/ doesn't get expired", 2, "log/log1/", "dir/dir1/"),
          arguments("dir/dir1/", "log/log1/", "data/data1/", "log/log1/log2/", true,
              "Prefix directory data/data1/ doesn't get expired", 3, "log/log1/", "data/data1/")
      );
    }

    @ParameterizedTest
    @MethodSource("parameters41")
    @SuppressWarnings("parameternumber")
    void testConsolidatedPrefixDirectoryNotExpired(String dir1, String dir2, String dir3, String dir4,
        boolean shouldConsolidateRule, String expectedLog, int consolidatedRuleListSize,
        String firstConsolidatedPrefix, String lastConsolidatedPrefix)
        throws IOException, TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyIterated = metrics.getNumKeyIterated().value();
      long initialKeyDeleted = metrics.getNumKeyDeleted().value();

      // Create the directories
      createVolumeAndBucket(volumeName, bucketName, FILE_SYSTEM_OPTIMIZED,
          UserGroupInformation.getCurrentUser().getShortUserName());
      createDirectory(volumeName, bucketName, dir1);
      createDirectory(volumeName, bucketName, dir2);
      KeyInfoWithVolumeContext keyInfo = getDirectory(volumeName, bucketName, dir1);
      assertFalse(keyInfo.getKeyInfo().isFile());
      keyInfo = getDirectory(volumeName, bucketName, dir2);
      assertFalse(keyInfo.getKeyInfo().isFile());
      List<OmKeyArgs> keyList = new ArrayList<>();
      keyList.add(createAndCommitKey(volumeName, bucketName, dir1 + "key1", 1, null));
      keyList.add(createAndCommitKey(volumeName, bucketName, dir2 + "key2", 1, null));

      Thread.sleep(SERVICE_INTERVAL);
      KeyLifecycleService.setTest(true);
      KeyLifecycleService.reSetConsolidatedRuleList();

      GenericTestUtils.LogCapturer log =
          GenericTestUtils.LogCapturer.captureLogs(
              LoggerFactory.getLogger(KeyLifecycleService.class));

      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      List<OmLCRule> ruleList = new ArrayList<>();
      String ruleID1 = String.valueOf(OBJECT_ID_COUNTER.getAndIncrement());
      String ruleID2 = String.valueOf(OBJECT_ID_COUNTER.getAndIncrement());
      ruleList.add(new OmLCRule.Builder().setId(ruleID1)
          .setEnabled(true).setPrefix(dir1)
          .setAction(new OmLCExpiration.Builder().setDate(date.toString()).build())
          .build());
      ruleList.add(new OmLCRule.Builder().setId(ruleID2)
          .setEnabled(true).setPrefix(dir2)
          .setAction(new OmLCExpiration.Builder().setDate(date.toString()).build())
          .build());
      if (dir3 != null) {
        String ruleID3 = String.valueOf(OBJECT_ID_COUNTER.getAndIncrement());
        ruleList.add(new OmLCRule.Builder().setId(ruleID3)
            .setEnabled(true).setPrefix(dir3)
            .setAction(new OmLCExpiration.Builder().setDate(date.toString()).build())
            .build());
        keyList.add(createAndCommitKey(volumeName, bucketName, dir3 + "key3", 1, null));
      }
      if (dir4 != null) {
        String ruleID4 = String.valueOf(OBJECT_ID_COUNTER.getAndIncrement());
        ruleList.add(new OmLCRule.Builder().setId(ruleID4)
            .setEnabled(true).setPrefix(dir4)
            .setAction(new OmLCExpiration.Builder().setDate(date.toString()).build())
            .build());
        keyList.add(createAndCommitKey(volumeName, bucketName, dir4 + "key4", 1, null));
      }
      createLifecyclePolicy(volumeName, bucketName, FILE_SYSTEM_OPTIMIZED, ruleList);

      try {
        if (shouldConsolidateRule) {
          GenericTestUtils.waitFor(() -> log.getOutput().contains("Consolidate"), WAIT_CHECK_INTERVAL, 5000);
        }
        if (expectedLog != null) {
          GenericTestUtils.waitFor(() -> log.getOutput().contains(expectedLog), WAIT_CHECK_INTERVAL, 5000);
        }
        GenericTestUtils.waitFor(() -> getDeletedKeyCount() - initialDeletedKeyCount == keyList.size(),
            WAIT_CHECK_INTERVAL, 5000);
        GenericTestUtils.waitFor(() -> keyList.size() == metrics.getNumKeyIterated().value() - initialKeyIterated,
            WAIT_CHECK_INTERVAL, 5000);
        GenericTestUtils.waitFor(() -> keyList.size() == metrics.getNumKeyDeleted().value() - initialKeyDeleted,
            WAIT_CHECK_INTERVAL, 5000);
        GenericTestUtils.waitFor(() -> {
          List<KeyLifecycleService.RuleListWithDirectoryList> list = KeyLifecycleService.getConsolidatedRuleList();
          boolean sizeMatch = list != null && list.size() == consolidatedRuleListSize;
          boolean firstPrefixMatch = list != null &&
              firstConsolidatedPrefix.equals(list.get(0).getConsolidatedPrefix());
          boolean lastPrefixMatch = lastConsolidatedPrefix == null ? true :
              list != null && lastConsolidatedPrefix.equals(list.get(list.size() - 1).getConsolidatedPrefix());
          return sizeMatch && firstPrefixMatch && lastPrefixMatch;
        }, WAIT_CHECK_INTERVAL, 5000);
      } finally {
        deleteLifecyclePolicy(volumeName, bucketName);
      }
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
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, "dir1/dir2/dir4/", null, date.toString(), true);

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
      List<OmLCRule> ruleList = new ArrayList<>();
      String rulePrefix = bucketLayout == FILE_SYSTEM_OPTIMIZED ? "" : keyPrefix;
      ruleList.add(new OmLCRule.Builder().setId(String.valueOf(OBJECT_ID_COUNTER.getAndIncrement()))
          .setEnabled(false).setPrefix(rulePrefix)
          .setAction(new OmLCExpiration.Builder().setDate(date.toString()).build())
          .build());
      ruleList.add(new OmLCRule.Builder().setId(String.valueOf(OBJECT_ID_COUNTER.getAndIncrement()))
          .setEnabled(true).setPrefix(rulePrefix)
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
      assumeTrue(stateSaveInternal != -1);
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      GenericTestUtils.LogCapturer log =
          GenericTestUtils.LogCapturer.captureLogs(
              LoggerFactory.getLogger(KeyLifecycleService.class));
      GenericTestUtils.LogCapturer requestLog =
          GenericTestUtils.LogCapturer.captureLogs(
              LoggerFactory.getLogger(OMKeysDeleteRequest.class));
      String keyPrefix = "key";
      String rulePrefix = bucketLayout == FILE_SYSTEM_OPTIMIZED ? "" : keyPrefix;
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, keyPrefix, null);

      KeyLifecycleService.setInjectors(
          Arrays.asList(new FaultInjectorImpl(), new FaultInjectorImpl()));

      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, rulePrefix, null, date.toString(), true);
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
      String keyPrefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      long initialKeyDeleted = metrics.getNumKeyDeleted().value();
      long initialDirIterated = metrics.getNumDirIterated().value();
      long initialDirDeleted = metrics.getNumDirDeleted().value();
      final int keyCount = 10;
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, keyCount, 1, keyPrefix, null);
      // check there are keys in keyTable
      assertEquals(keyCount, keyList.size());
      GenericTestUtils.waitFor(() -> getKeyCount(bucketLayout) - initialKeyCount == keyCount,
          WAIT_CHECK_INTERVAL, 1000);
      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      List<OmLCRule> ruleList = new ArrayList<>();
      String rulePrefix = bucketLayout == FILE_SYSTEM_OPTIMIZED ? "" : keyPrefix;
      ruleList.add(new OmLCRule.Builder().setId(String.valueOf(OBJECT_ID_COUNTER.getAndIncrement()))
          .setEnabled(false).setPrefix(rulePrefix)
          .setAction(new OmLCExpiration.Builder().setDate(date.toString()).build())
          .build());
      ruleList.add(new OmLCRule.Builder().setId(String.valueOf(OBJECT_ID_COUNTER.getAndIncrement()))
          .setEnabled(true).setPrefix(rulePrefix)
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
      GenericTestUtils.waitFor(() -> metrics.getNumDirDeleted().value() - initialDirDeleted == 0,
          WAIT_CHECK_INTERVAL, 5000);
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
      String keyPrefix = "key";
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      long initialRenamedKeyCount = metrics.getNumKeyRenamed().value();
      final int keyCount = 100;
      final int maxListSize = 20;
      keyLifecycleService.setListMaxSize(maxListSize);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, keyCount, 1, keyPrefix, null);
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
      String rulePrefix = bucketLayout == FILE_SYSTEM_OPTIMIZED ? "" : keyPrefix;
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, rulePrefix, null, date.toString(), true);

      if (enableTrash && bucketLayout != OBJECT_STORE) {
        GenericTestUtils.waitFor(() ->
            (metrics.getNumKeyRenamed().value() - initialRenamedKeyCount) == keyCount, WAIT_CHECK_INTERVAL, 5000);
        assertEquals(0, getDeletedKeyCount() - initialDeletedKeyCount);
      } else {
        GenericTestUtils.waitFor(() ->
            (getDeletedKeyCount() - initialDeletedKeyCount) == keyCount, WAIT_CHECK_INTERVAL, 5000);
        assertEquals(0, getKeyCount(bucketLayout) - initialKeyCount);
      }
      if (stateSaveInternal != -1) {
        GenericTestUtils.waitFor(() ->
                log.getOutput().contains("LimitedSizeList has reached maximum size " + maxListSize),
            WAIT_CHECK_INTERVAL, 5000);
      }
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

      try {
        GenericTestUtils.waitFor(() ->
            (metrics.getNumKeyRenamed().value() - initialRenamedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 5000);
        assertEquals(0, getDeletedKeyCount() - initialDeletedKeyCount);
        if (bucketLayout == FILE_SYSTEM_OPTIMIZED) {
          // Legacy bucket doesn't have dir concept
          GenericTestUtils.waitFor(() ->
                  metrics.getNumDirRenamed().value() - initialRenamedDirCount == (prefix.contains(OM_KEY_PREFIX) ?
                      1 : 0), WAIT_CHECK_INTERVAL, 5000);
        }

        // verify that trash directory has the right native ACLs
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

        // keys under trash directory is counted in getKeyCount()
        if (bucketLayout == FILE_SYSTEM_OPTIMIZED) {
          assertEquals(KEY_COUNT, getKeyCount(bucketLayout) - initialKeyCount);
        } else {
          // For legacy bucket, trash directories along .Trash/user-test/Current are in key table too.
          assertEquals(KEY_COUNT + (prefix.contains(OM_KEY_PREFIX) ? 4 : 3),
              getKeyCount(bucketLayout) - initialKeyCount);
        }
      } finally {
        deleteLifecyclePolicy(volumeName, bucketName);
      }
    }

    @ParameterizedTest
    @MethodSource("parameters7")
    void testMoveToTrashWithTrashPrefix(BucketLayout bucketLayout, String prefix) throws IOException,
        TimeoutException, InterruptedException {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      long initialKeyCount = getKeyCount(bucketLayout);
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

      // create a new policy to test rule with prefix ".Trash/" is ignored during lifecycle evaluation
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      final String expiredDate = now.plusSeconds(EXPIRE_SECONDS).toString();
      assertThrowsExactly(OMException.class, () -> createLifecyclePolicy(
          volumeName, bucketName, bucketLayout, TRASH_PREFIX + OM_KEY_PREFIX, null, expiredDate, true));

      // create a new policy to test rule with prefix ".Trash" is ignored during lifecycle evaluation
      assertThrowsExactly(OMException.class, () -> createLifecyclePolicy(
          volumeName, bucketName, bucketLayout, TRASH_PREFIX,
          null, expiredDate, true));

      // create a new policy to test rule with prefix ".Tras/" is ignored during lifecycle evaluation
      now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, FILE_SYSTEM_OPTIMIZED, ".Tras/", null, date.toString(), true);

      GenericTestUtils.LogCapturer log =
          GenericTestUtils.LogCapturer.captureLogs(
              LoggerFactory.getLogger(KeyLifecycleService.class));
      GenericTestUtils.waitFor(
          () -> log.getOutput().contains("No expired keys/dirs found/remained for bucket"), WAIT_CHECK_INTERVAL, 5000);
      deleteLifecyclePolicy(volumeName, bucketName);
      log.clearOutput();

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
          // dir3 and keys under dir3 deleted
          arguments("dir1/dir2/dir3/key", null, "dir1/dir2/", "dir1/dir2/dir3/", KEY_COUNT, 1, false),
          // no dir, but keys under dir3 deleted
          arguments("dir1/dir2/dir3/key", null, "dir1/dir2/", "dir1/dir2/dir3/", KEY_COUNT, 0, true),
          // dir3 dir5, and all keys under dir3 and dir5 deleted
          arguments("dir1/dir2/dir3/key", "dir1/dir2/dir5/key", "dir1/dir2/", "dir1/dir2/dir3",
              KEY_COUNT * 2, 2, false),
          // dir5, and all keys under dir3 and dir5 deleted
          arguments("dir1/dir2/dir3/key", "dir1/dir2/dir5/key", "dir1/dir2/", "dir1/dir2/dir3", KEY_COUNT * 2, 1, true),
          // dir2 dir3 dir4 dir5, and all keys under dir3 and dir5 deleted
          arguments("dir1/dir2/dir3/key", "dir1/dir4/dir5/key", "dir1/", "dir1/dir2/dir3", KEY_COUNT * 2, 4, false),
          // dir4 dir5, and all keys under dir3 and dir5 deleted
          arguments("dir1/dir2/dir3/key", "dir1/dir4/dir5/key", "dir1/", "dir1/dir2/dir3", KEY_COUNT * 2, 2, true),
          // dir1 - dir5, and all keys deleted
          arguments("dir1/dir2/dir3/key", "dir1/dir4/dir5/key", "", "dir1/dir2/dir3", KEY_COUNT * 2, 5, false),
          // dir4 dir5, and all keys deleted
          arguments("dir1/dir2/dir3/key", "dir1/dir4/dir5/key", "", "dir1/dir2/dir3", KEY_COUNT * 2, 2, true),
          // dir4 dir5, and all keys under dir5 deleted
          arguments("dir11/dir4/dir5/key", "dir1/dir2/dir3/key", "dir11/", "dir11/dir4/dir5", KEY_COUNT, 2, false),
          // no dir, but all keys under dir11 deleted
          arguments("dir11/dir4/dir5/key", "dir1/dir2/dir3/key", "dir11/", "dir11/dir4/dir5", KEY_COUNT, 0, true));
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
      GenericTestUtils.waitFor(() -> endDate.isBefore(ZonedDateTime.now(ZoneOffset.UTC)), WAIT_CHECK_INTERVAL, 5000);

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
      createLifecyclePolicy(volumeName, bucketName, FILE_SYSTEM_OPTIMIZED, "", null, date.toString(), true);
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

      // Make trash available, but disable move.to.trash in KeyLifecycleService.
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

      // With move.to.trash disabled, keys should be deleted directly (not renamed).
      GenericTestUtils.waitFor(() ->
          (getDeletedKeyCount() - initialDeletedKeyCount) == KEY_COUNT, WAIT_CHECK_INTERVAL, 10000);
      assertEquals(initialRenamedKeyCount, metrics.getNumKeyRenamed().value());
      assertEquals(0, getKeyCount(FILE_SYSTEM_OPTIMIZED) - initialKeyCount);
      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @Test
    void testAbortIncompleteMultipartUploadWithFilters() throws Exception {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");

      // Create volume and bucket
      createVolumeAndBucket(volumeName, bucketName, OBJECT_STORE,
          UserGroupInformation.getCurrentUser().getShortUserName());

      String owner = UserGroupInformation.getCurrentUser().getShortUserName();

      long initialMpuCount = getMultipartUploadCount(volumeName, bucketName);

      // Create multipart uploads with different prefixes
      OmMultipartInfo mpuInfo1 = createTestMultipartUpload(volumeName, bucketName,
          "uploads/file1", owner);  // should match "uploads/" prefix rule
      OmMultipartInfo mpuInfo2 = createTestMultipartUpload(volumeName, bucketName,
          "uploads/file2", owner);  // should match "uploads/" prefix rule
      OmMultipartInfo mpuInfo3 = createTestMultipartUpload(volumeName, bucketName,
          "temp/file3", owner);     // should match "temp/" prefix rule
      OmMultipartInfo mpuInfo4 = createTestMultipartUpload(volumeName, bucketName,
          "keep/file4", owner);     // should NOT match any rule

      // Update creation time to be 2 days ago for all MPUs so they are eligible for abort
      long oldCreationTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(2);
      updateMultipartUploadCreationTime(volumeName, bucketName, "uploads/file1",
          mpuInfo1.getUploadID(), oldCreationTime);
      updateMultipartUploadCreationTime(volumeName, bucketName, "uploads/file2",
          mpuInfo2.getUploadID(), oldCreationTime);
      updateMultipartUploadCreationTime(volumeName, bucketName, "temp/file3",
          mpuInfo3.getUploadID(), oldCreationTime);
      updateMultipartUploadCreationTime(volumeName, bucketName, "keep/file4",
          mpuInfo4.getUploadID(), oldCreationTime);

      List<OmLCRule> rules = new ArrayList<>();

      // Rule 1: Abort MPUs with prefix "uploads/" after 1 day
      OmLCRule rule1 = new OmLCRule.Builder()
          .setId("abort-mpu-uploads")
          .setEnabled(true)
          .setFilter(new OmLCFilter.Builder()
              .setPrefix("uploads/")
              .build())
          .setAction(new OmLCAbortIncompleteMultipartUpload.Builder()
              .setDaysAfterInitiation(1)
              .build())
          .build();
      rules.add(rule1);

      // Rule 2: Abort MPUs with prefix "temp/" after 1 day
      OmLCRule rule2 = new OmLCRule.Builder()
          .setId("abort-mpu-temp")
          .setEnabled(true)
          .setFilter(new OmLCFilter.Builder()
              .setPrefix("temp/")
              .build())
          .setAction(new OmLCAbortIncompleteMultipartUpload.Builder()
              .setDaysAfterInitiation(1)
              .build())
          .build();
      rules.add(rule2);

      // Validate the rules have AbortIncompleteMultipartUpload actions
      for (OmLCRule rule : rules) {
        assertNotNull(rule.getAbortIncompleteMultipartUpload(),
            "Rule should have AbortIncompleteMultipartUpload action");
        assertEquals(1, rule.getAbortIncompleteMultipartUpload().getDaysAfterInitiation());
      }

      createLifecyclePolicy(volumeName, bucketName, OBJECT_STORE, rules);

      // Verify lifecycle configuration was stored correctly
      String lcKey = "/" + volumeName + "/" + bucketName;
      OmLifecycleConfiguration storedConfig = metadataManager.getLifecycleConfigurationTable()
          .get(lcKey);
      assertNotNull(storedConfig, "Lifecycle configuration should be stored");
      assertEquals(2, storedConfig.getRules().size(), "Should have 2 rules");

      // Verify rules preserve AbortIncompleteMultipartUpload actions after serialization/deserialization
      for (OmLCRule rule : storedConfig.getRules()) {
        assertNotNull(rule.getAbortIncompleteMultipartUpload(),
            "Stored rule should have AbortIncompleteMultipartUpload action");
        assertEquals(1, rule.getAbortIncompleteMultipartUpload().getDaysAfterInitiation());
      }

      // Verify the lifecycle configuration is valid
      storedConfig.valid();

      // Wait for lifecycle service to abort the matching MPUs
      // 3 MPUs should be aborted (uploads/file1, uploads/file2, temp/file3)
      // 1 MPU should remain (keep/file4)
      GenericTestUtils.waitFor(() ->
          getMultipartUploadCount(volumeName, bucketName) - initialMpuCount == 1,
          WAIT_CHECK_INTERVAL, 10000);

      // Verify only the non-matching MPU remains
      String keepMpuKey = metadataManager.getMultipartKey(volumeName, bucketName,
          "keep/file4", mpuInfo4.getUploadID());
      assertNotNull(metadataManager.getMultipartInfoTable().get(keepMpuKey),
          "MPU with prefix 'keep/' should NOT be aborted");

      // Verify matching MPUs are aborted
      String abortedKey1 = metadataManager.getMultipartKey(volumeName, bucketName,
          "uploads/file1", mpuInfo1.getUploadID());
      assertNull(metadataManager.getMultipartInfoTable().get(abortedKey1),
          "MPU with prefix 'uploads/' should be aborted");

      String abortedKey2 = metadataManager.getMultipartKey(volumeName, bucketName,
          "uploads/file2", mpuInfo2.getUploadID());
      assertNull(metadataManager.getMultipartInfoTable().get(abortedKey2),
          "MPU with prefix 'uploads/' should be aborted");

      String abortedKey3 = metadataManager.getMultipartKey(volumeName, bucketName,
          "temp/file3", mpuInfo3.getUploadID());
      assertNull(metadataManager.getMultipartInfoTable().get(abortedKey3),
          "MPU with prefix 'temp/' should be aborted");

      deleteLifecyclePolicy(volumeName, bucketName);
    }

    @Test
    void testAbortIncompleteMultipartUploadWithTagFilter() throws Exception {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");

      // Create volume and bucket
      createVolumeAndBucket(volumeName, bucketName, OBJECT_STORE,
          UserGroupInformation.getCurrentUser().getShortUserName());

      String owner = UserGroupInformation.getCurrentUser().getShortUserName();

      // Record initial MPU count for this bucket
      long initialMpuCount = getMultipartUploadCount(volumeName, bucketName);

      // Create multipart uploads with different tags
      // MPU 1: has matching tag (environment=test) - should be aborted
      OmKeyArgs keyArgs1 = new OmKeyArgs.Builder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setKeyName("file1.txt")
          .setAcls(Collections.emptyList())
          .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
          .setLocationInfoList(new ArrayList<>())
          .setOwnerName(owner)
          .addTag("environment", "test")
          .build();
      OmMultipartInfo mpuInfo1 = writeClient.initiateMultipartUpload(keyArgs1);

      // MPU 2: has matching tag (environment=test) - should be aborted
      OmKeyArgs keyArgs2 = new OmKeyArgs.Builder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setKeyName("file2.txt")
          .setAcls(Collections.emptyList())
          .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
          .setLocationInfoList(new ArrayList<>())
          .setOwnerName(owner)
          .addTag("environment", "test")
          .build();
      OmMultipartInfo mpuInfo2 = writeClient.initiateMultipartUpload(keyArgs2);

      // MPU 3: has non-matching tag value (environment=prod) - should NOT be aborted
      OmKeyArgs keyArgs3 = new OmKeyArgs.Builder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setKeyName("file3.txt")
          .setAcls(Collections.emptyList())
          .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
          .setLocationInfoList(new ArrayList<>())
          .setOwnerName(owner)
          .addTag("environment", "prod")
          .build();
      OmMultipartInfo mpuInfo3 = writeClient.initiateMultipartUpload(keyArgs3);

      // MPU 4: has no tags - should NOT be aborted
      OmKeyArgs keyArgs4 = new OmKeyArgs.Builder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setKeyName("file4.txt")
          .setAcls(Collections.emptyList())
          .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
          .setLocationInfoList(new ArrayList<>())
          .setOwnerName(owner)
          .build();
      OmMultipartInfo mpuInfo4 = writeClient.initiateMultipartUpload(keyArgs4);

      // Update creation time to be 2 days ago for all MPUs so they are eligible for abort
      long oldCreationTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(2);
      updateMultipartUploadCreationTime(volumeName, bucketName, "file1.txt",
          mpuInfo1.getUploadID(), oldCreationTime);
      updateMultipartUploadCreationTime(volumeName, bucketName, "file2.txt",
          mpuInfo2.getUploadID(), oldCreationTime);
      updateMultipartUploadCreationTime(volumeName, bucketName, "file3.txt",
          mpuInfo3.getUploadID(), oldCreationTime);
      updateMultipartUploadCreationTime(volumeName, bucketName, "file4.txt",
          mpuInfo4.getUploadID(), oldCreationTime);

      List<OmLCRule> rules = new ArrayList<>();

      // Rule: Abort MPUs with tag environment=test after 1 day
      OmLCRule rule = new OmLCRule.Builder()
          .setId("abort-mpu-test-env")
          .setEnabled(true)
          .setFilter(new OmLCFilter.Builder()
              .setTag("environment", "test")
              .build())
          .setAction(new OmLCAbortIncompleteMultipartUpload.Builder()
              .setDaysAfterInitiation(1)
              .build())
          .build();
      rules.add(rule);

      // Validate the rule has AbortIncompleteMultipartUpload action
      assertNotNull(rule.getAbortIncompleteMultipartUpload(),
          "Rule should have AbortIncompleteMultipartUpload action");

      createLifecyclePolicy(volumeName, bucketName, OBJECT_STORE, rules);

      // Wait for lifecycle service to abort the matching MPUs
      // 2 MPUs should be aborted (file1.txt and file2.txt with environment=test)
      // 2 MPUs should remain (file3.txt with environment=prod and file4.txt with no tags)
      GenericTestUtils.waitFor(() ->
          getMultipartUploadCount(volumeName, bucketName) - initialMpuCount == 2,
          WAIT_CHECK_INTERVAL, 10000);

      // Verify non-matching MPUs remain
      String remainKey3 = metadataManager.getMultipartKey(volumeName, bucketName,
          "file3.txt", mpuInfo3.getUploadID());
      assertNotNull(metadataManager.getMultipartInfoTable().get(remainKey3),
          "MPU with tag 'environment=prod' should NOT be aborted");

      String remainKey4 = metadataManager.getMultipartKey(volumeName, bucketName,
          "file4.txt", mpuInfo4.getUploadID());
      assertNotNull(metadataManager.getMultipartInfoTable().get(remainKey4),
          "MPU without tags should NOT be aborted");

      // Verify matching MPUs are aborted
      String abortedKey1 = metadataManager.getMultipartKey(volumeName, bucketName,
          "file1.txt", mpuInfo1.getUploadID());
      assertNull(metadataManager.getMultipartInfoTable().get(abortedKey1),
          "MPU with tag 'environment=test' should be aborted");

      String abortedKey2 = metadataManager.getMultipartKey(volumeName, bucketName,
          "file2.txt", mpuInfo2.getUploadID());
      assertNull(metadataManager.getMultipartInfoTable().get(abortedKey2),
          "MPU with tag 'environment=test' should be aborted");

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
      assumeTrue(stateSaveInternal != -1);
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      GenericTestUtils.LogCapturer log =
          GenericTestUtils.LogCapturer.captureLogs(
              LoggerFactory.getLogger(KeyLifecycleService.class));
      GenericTestUtils.LogCapturer requestLog =
          GenericTestUtils.LogCapturer.captureLogs(
              LoggerFactory.getLogger(OMKeysDeleteRequest.class));
      String keyPrefix = "key";
      String rulePrefix = bucketLayout == FILE_SYSTEM_OPTIMIZED ? "" : keyPrefix;
      long initialDeletedKeyCount = getDeletedKeyCount();
      long initialKeyCount = getKeyCount(bucketLayout);
      // create keys
      List<OmKeyArgs> keyList =
          createKeys(volumeName, bucketName, bucketLayout, KEY_COUNT, 1, keyPrefix, null);

      KeyLifecycleService.setInjectors(
          Arrays.asList(new FaultInjectorImpl(), new FaultInjectorImpl()));

      // create Lifecycle configuration
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime date = now.plusSeconds(EXPIRE_SECONDS);
      createLifecyclePolicy(volumeName, bucketName, bucketLayout, rulePrefix, null, date.toString(), true);
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
    OmLifecycleConfiguration lcc;
    try {
      lcc = new OmLifecycleConfiguration.Builder()
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
    } catch (IllegalArgumentException e) {
      if (e.getCause() instanceof OMException) {
        throw (OMException) e.getCause();
      }
      throw e;
    }
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
    OmLifecycleConfiguration lcc;
    try {
      lcc = new OmLifecycleConfiguration.Builder()
          .setVolume(volume)
          .setBucket(bucket)
          .setBucketObjectID(bucketObjectID)
          .setBucketLayout(layout)
          .setRules(ruleList)
          .build();
    } catch (IllegalArgumentException e) {
      if (e.getCause() instanceof OMException) {
        throw (OMException) e.getCause();
      }
      throw e;
    }
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

  private long getMultipartUploadCount(String volumeName, String bucketName) {
    String prefix = metadataManager.getBucketKeyPrefix(volumeName, bucketName);
    long count = 0;
    try (TableIterator<String, ? extends Table.KeyValue<String, OmMultipartKeyInfo>> iter =
             metadataManager.getMultipartInfoTable().iterator(prefix)) {
      while (iter.hasNext()) {
        Table.KeyValue<String, OmMultipartKeyInfo> entry = iter.next();
        // Check if the key starts with the prefix (belongs to this bucket)
        if (entry.getKey().startsWith(prefix)) {
          count++;
        } else {
          break;  // Iterator went past our prefix
        }
      }
    } catch (IOException e) {
      fail("Failed to count multipart uploads: " + e.getMessage());
      return -1;
    }
    return count;
  }

  private void updateMultipartUploadCreationTime(String volumeName, String bucketName,
      String keyName, String uploadId, long newCreationTime) throws IOException {
    String dbKey = metadataManager.getMultipartKey(volumeName, bucketName, keyName, uploadId);
    OmMultipartKeyInfo existingInfo = metadataManager.getMultipartInfoTable().get(dbKey);
    if (existingInfo == null) {
      fail("Multipart upload not found: " + dbKey);
      return;
    }

    // Create a new OmMultipartKeyInfo with the updated creation time
    OmMultipartKeyInfo updatedInfo = new OmMultipartKeyInfo.Builder()
        .setUploadID(existingInfo.getUploadID())
        .setCreationTime(newCreationTime)
        .setReplicationConfig(existingInfo.getReplicationConfig())
        .setObjectID(existingInfo.getObjectID())
        .setUpdateID(existingInfo.getUpdateID())
        .setParentID(existingInfo.getParentID())
        .build();

    // Copy part key infos
    for (OzoneManagerProtocolProtos.PartKeyInfo partKeyInfo : existingInfo.getPartKeyInfoMap()) {
      updatedInfo.addPartKeyInfo(partKeyInfo);
    }

    metadataManager.getMultipartInfoTable().put(dbKey, updatedInfo);
  }

  private OmMultipartInfo createTestMultipartUpload(String volumeName, String bucketName,
      String keyName, String owner) throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setAcls(Collections.emptyList())
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setLocationInfoList(new ArrayList<>())
        .setOwnerName(owner)
        .build();
    return writeClient.initiateMultipartUpload(keyArgs);
  }

  public static String uniqueObjectName(String prefix) {
    return prefix + OBJECT_COUNTER.getAndIncrement();
  }
}

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

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.api.types.ContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.KeyPrefixContainer;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit Tests for ContainerDBServiceProviderImpl.
 */
public class TestReconContainerMetadataManagerImpl {

  @TempDir()
  private static Path temporaryFolder;
  private static ReconContainerMetadataManager reconContainerMetadataManager;

  private String keyPrefix1 = "V3/B1/K1";
  private String keyPrefix2 = "V3/B1/K2";
  private String keyPrefix3 = "V3/B2/K1";

  @BeforeAll
  public static void setupOnce() throws Exception {
    ReconOMMetadataManager reconOMMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(Files.createDirectory(
            temporaryFolder.resolve("JunitOmDBDir")).toFile()),
        Files.createDirectory(temporaryFolder.resolve("NewDir")).toFile());
    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder.toFile())
            .withReconSqlDb()
            .withContainerDB()
            .withReconOm(reconOMMetadataManager)
            .build();
    reconContainerMetadataManager =
        reconTestInjector.getInstance(ReconContainerMetadataManager.class);
  }

  @BeforeEach
  public void setUp() throws Exception {
    // Reset containerDB before running each test
    reconContainerMetadataManager.reinitWithNewContainerDataFromOm(null);
  }

  private void populateKeysInContainers(long containerId1, long containerId2)
      throws Exception {

    RDBBatchOperation rdbBatchOperation = RDBBatchOperation.newAtomicOperation();
    ContainerKeyPrefix containerKeyPrefix1 = ContainerKeyPrefix.get(
        containerId1, keyPrefix1, 0);
    reconContainerMetadataManager
        .batchStoreContainerKeyMapping(rdbBatchOperation, containerKeyPrefix1,
            1);

    ContainerKeyPrefix containerKeyPrefix2 = ContainerKeyPrefix.get(
        containerId1, keyPrefix2, 0);
    reconContainerMetadataManager
        .batchStoreContainerKeyMapping(rdbBatchOperation, containerKeyPrefix2,
        2);

    ContainerKeyPrefix containerKeyPrefix3 = ContainerKeyPrefix.get(
        containerId2, keyPrefix3, 0);
    reconContainerMetadataManager
        .batchStoreContainerKeyMapping(rdbBatchOperation, containerKeyPrefix3,
        3);

    reconContainerMetadataManager.commitBatchOperation(rdbBatchOperation);
  }

  @Test
  public void testInitNewContainerDB() throws Exception {
    long containerId = System.currentTimeMillis();
    Map<ContainerKeyPrefix, Integer> prefixCounts = new HashMap<>();

    ContainerKeyPrefix ckp1 = ContainerKeyPrefix.get(containerId,
        "V1/B1/K1", 0);
    prefixCounts.put(ckp1, 1);

    ContainerKeyPrefix ckp2 = ContainerKeyPrefix.get(containerId,
        "V1/B1/K2", 0);
    prefixCounts.put(ckp2, 2);

    ContainerKeyPrefix ckp3 = ContainerKeyPrefix.get(containerId,
        "V1/B2/K3", 0);
    prefixCounts.put(ckp3, 3);

    RDBBatchOperation rdbBatchOperation = RDBBatchOperation.newAtomicOperation();
    for (Map.Entry<ContainerKeyPrefix, Integer> entry :
        prefixCounts.entrySet()) {
      reconContainerMetadataManager.batchStoreContainerKeyMapping(
          rdbBatchOperation, entry.getKey(), prefixCounts.get(entry.getKey()));
    }
    reconContainerMetadataManager.commitBatchOperation(rdbBatchOperation);

    assertEquals(1, reconContainerMetadataManager
        .getCountForContainerKeyPrefix(ckp1).intValue());

    prefixCounts.clear();
    prefixCounts.put(ckp2, 12);
    prefixCounts.put(ckp3, 13);
    ContainerKeyPrefix ckp4 = ContainerKeyPrefix.get(containerId,
        "V1/B3/K1", 0);
    prefixCounts.put(ckp4, 14);
    ContainerKeyPrefix ckp5 = ContainerKeyPrefix.get(containerId,
        "V1/B3/K2", 0);
    prefixCounts.put(ckp5, 15);

    reconContainerMetadataManager
            .reinitWithNewContainerDataFromOm(prefixCounts);
    Map<ContainerKeyPrefix, Integer> keyPrefixesForContainer =
        reconContainerMetadataManager.getKeyPrefixesForContainer(containerId);

    assertEquals(4, keyPrefixesForContainer.size());
    assertEquals(12, keyPrefixesForContainer.get(ckp2).intValue());
    assertEquals(13, keyPrefixesForContainer.get(ckp3).intValue());
    assertEquals(14, keyPrefixesForContainer.get(ckp4).intValue());
    assertEquals(15, keyPrefixesForContainer.get(ckp5).intValue());

    assertEquals(0, reconContainerMetadataManager
        .getCountForContainerKeyPrefix(ckp1).intValue());
  }

  @Test
  public void testBatchStoreContainerKeyMapping() throws Exception {

    long containerId = System.currentTimeMillis();
    Map<String, Integer> prefixCounts = new HashMap<>();
    prefixCounts.put(keyPrefix1, 1);
    prefixCounts.put(keyPrefix2, 2);
    prefixCounts.put(keyPrefix3, 3);

    RDBBatchOperation rdbBatchOperation = RDBBatchOperation.newAtomicOperation();
    for (Map.Entry<String, Integer> entry : prefixCounts.entrySet()) {
      ContainerKeyPrefix containerKeyPrefix = ContainerKeyPrefix.get(
          containerId, entry.getKey(), 0);
      reconContainerMetadataManager.batchStoreContainerKeyMapping(
          rdbBatchOperation, containerKeyPrefix,
          prefixCounts.get(entry.getKey()));
    }
    reconContainerMetadataManager.commitBatchOperation(rdbBatchOperation);

    assertEquals(1,
        reconContainerMetadataManager.getCountForContainerKeyPrefix(
            ContainerKeyPrefix.get(containerId, keyPrefix1,
                0)).longValue());
    assertEquals(2,
        reconContainerMetadataManager.getCountForContainerKeyPrefix(
            ContainerKeyPrefix.get(containerId, keyPrefix2,
                0)).longValue());
    assertEquals(3,
        reconContainerMetadataManager.getCountForContainerKeyPrefix(
            ContainerKeyPrefix.get(containerId, keyPrefix3,
                0)).longValue());
  }

  @Test
  public void testStoreContainerKeyCount() throws Exception {
    long containerId = 1L;
    long nextContainerId = 2L;
    RDBBatchOperation rdbBatchOperation = RDBBatchOperation.newAtomicOperation();
    reconContainerMetadataManager
        .batchStoreContainerKeyCounts(rdbBatchOperation, containerId, 2L);
    reconContainerMetadataManager
        .batchStoreContainerKeyCounts(rdbBatchOperation, nextContainerId, 3L);
    reconContainerMetadataManager.commitBatchOperation(rdbBatchOperation);

    assertEquals(2,
        reconContainerMetadataManager.getKeyCountForContainer(containerId));
    assertEquals(3,
        reconContainerMetadataManager.getKeyCountForContainer(nextContainerId));

    RDBBatchOperation rdbBatchOperation2 = RDBBatchOperation.newAtomicOperation();
    reconContainerMetadataManager
        .batchStoreContainerKeyCounts(rdbBatchOperation2, containerId, 20L);
    reconContainerMetadataManager.commitBatchOperation(rdbBatchOperation2);
    assertEquals(20,
        reconContainerMetadataManager.getKeyCountForContainer(containerId));
  }

  @Test
  public void testGetKeyCountForContainer() throws Exception {
    long containerId = 1L;
    long nextContainerId = 2L;
    RDBBatchOperation rdbBatchOperation = RDBBatchOperation.newAtomicOperation();
    reconContainerMetadataManager
        .batchStoreContainerKeyCounts(rdbBatchOperation, containerId, 2L);
    reconContainerMetadataManager
        .batchStoreContainerKeyCounts(rdbBatchOperation, nextContainerId, 3L);
    reconContainerMetadataManager.commitBatchOperation(rdbBatchOperation);

    assertEquals(2,
        reconContainerMetadataManager.getKeyCountForContainer(containerId));
    assertEquals(3,
        reconContainerMetadataManager.getKeyCountForContainer(nextContainerId));

    assertEquals(0,
        reconContainerMetadataManager.getKeyCountForContainer(5L));
  }

  @Test
  public void testDoesContainerExists() throws Exception {
    long containerId = 1L;
    long nextContainerId = 2L;
    RDBBatchOperation rdbBatchOperation = RDBBatchOperation.newAtomicOperation();
    reconContainerMetadataManager
        .batchStoreContainerKeyCounts(rdbBatchOperation, containerId, 2L);
    reconContainerMetadataManager
        .batchStoreContainerKeyCounts(rdbBatchOperation, nextContainerId, 3L);
    reconContainerMetadataManager.commitBatchOperation(rdbBatchOperation);

    assertTrue(reconContainerMetadataManager.doesContainerExists(containerId));
    assertTrue(reconContainerMetadataManager.
            doesContainerExists(nextContainerId));
    assertFalse(reconContainerMetadataManager.doesContainerExists(0L));
    assertFalse(reconContainerMetadataManager.doesContainerExists(3L));
  }

  @Test
  public void testGetCountForContainerKeyPrefix() throws Exception {
    long containerId = System.currentTimeMillis();

    RDBBatchOperation rdbBatchOperation = RDBBatchOperation.newAtomicOperation();
    reconContainerMetadataManager.batchStoreContainerKeyMapping(
        rdbBatchOperation, ContainerKeyPrefix.get(containerId, keyPrefix1), 2);
    reconContainerMetadataManager.commitBatchOperation(rdbBatchOperation);

    Integer count = reconContainerMetadataManager.
        getCountForContainerKeyPrefix(ContainerKeyPrefix.get(containerId,
            keyPrefix1));
    assertEquals(2L, count.longValue());

    count = reconContainerMetadataManager.
        getCountForContainerKeyPrefix(ContainerKeyPrefix.get(containerId,
            "invalid"));
    assertEquals(0L, count.longValue());
  }

  @Test
  public void testGetKeyPrefixesForContainer() throws Exception {
    long containerId = 1L;
    long nextContainerId = 2L;
    populateKeysInContainers(containerId, nextContainerId);

    ContainerKeyPrefix containerKeyPrefix1 = ContainerKeyPrefix.get(
        containerId, keyPrefix1, 0);
    ContainerKeyPrefix containerKeyPrefix2 = ContainerKeyPrefix.get(
        containerId, keyPrefix2, 0);
    ContainerKeyPrefix containerKeyPrefix3 = ContainerKeyPrefix.get(
        nextContainerId, keyPrefix3, 0);


    Map<ContainerKeyPrefix, Integer> keyPrefixMap =
        reconContainerMetadataManager.getKeyPrefixesForContainer(containerId);
    assertEquals(2, keyPrefixMap.size());

    assertEquals(1, keyPrefixMap.get(containerKeyPrefix1).longValue());
    assertEquals(2, keyPrefixMap.get(containerKeyPrefix2).longValue());

    keyPrefixMap = reconContainerMetadataManager.getKeyPrefixesForContainer(
        nextContainerId);
    assertEquals(1, keyPrefixMap.size());
    assertEquals(3, keyPrefixMap.get(containerKeyPrefix3).longValue());
  }

  @Test
  public void testGetKeyPrefixesForContainerWithKeyPrefix() throws Exception {
    long containerId = 1L;
    long nextContainerId = 2L;
    populateKeysInContainers(containerId, nextContainerId);

    ContainerKeyPrefix containerKeyPrefix2 = ContainerKeyPrefix.get(
        containerId, keyPrefix2, 0);

    Map<ContainerKeyPrefix, Integer> keyPrefixMap =
        reconContainerMetadataManager.getKeyPrefixesForContainer(containerId,
            keyPrefix1, 1000);
    assertEquals(1, keyPrefixMap.size());
    assertEquals(2, keyPrefixMap.get(containerKeyPrefix2).longValue());

    keyPrefixMap = reconContainerMetadataManager.getKeyPrefixesForContainer(
        nextContainerId, keyPrefix3, 1000);
    assertEquals(0, keyPrefixMap.size());

    // test for negative cases
    keyPrefixMap = reconContainerMetadataManager.getKeyPrefixesForContainer(
        containerId, "V3/B1/invalid", 1000);
    assertEquals(0, keyPrefixMap.size());

    keyPrefixMap = reconContainerMetadataManager.getKeyPrefixesForContainer(
        containerId, keyPrefix3, 1000);
    assertEquals(0, keyPrefixMap.size());

    keyPrefixMap = reconContainerMetadataManager.getKeyPrefixesForContainer(
        10L, "", 1000);
    assertEquals(0, keyPrefixMap.size());
  }

  @Test
  public void testGetContainerForKeyPrefixesWithKeyPrefix() throws Exception {
    long containerId = 1L;
    long nextContainerId = 2L;
    populateKeysInContainers(containerId, nextContainerId);

    Map<KeyPrefixContainer, Integer> keyPrefixMap =
        reconContainerMetadataManager.getContainerForKeyPrefixes(keyPrefix1, 0);
    assertEquals(1, keyPrefixMap.size());

    keyPrefixMap = reconContainerMetadataManager.getContainerForKeyPrefixes(
        keyPrefix3, 0);
    assertEquals(1, keyPrefixMap.size());

    keyPrefixMap = reconContainerMetadataManager.getContainerForKeyPrefixes(
        keyPrefix3, 2);
    assertEquals(0, keyPrefixMap.size());

    // test for negative cases
    keyPrefixMap = reconContainerMetadataManager.getContainerForKeyPrefixes(
        "V3/B1/invalid", -1);
    assertEquals(0, keyPrefixMap.size());
  }

  @Test
  public void testGetContainersWithPrevContainer() throws Exception {
    long containerId = 1L;
    long nextContainerId = 2L;
    populateKeysInContainers(containerId, nextContainerId);

    Map<Long, ContainerMetadata> containerMap =
        reconContainerMetadataManager.getContainers(-1, 0L);
    assertEquals(2, containerMap.size());

    assertEquals(2, containerMap.get(containerId).getNumberOfKeys());
    assertEquals(1, containerMap.get(nextContainerId).getNumberOfKeys());

    // test if limit works
    containerMap = reconContainerMetadataManager.getContainers(
        1, 0L);
    assertEquals(1, containerMap.size());
    assertNull(containerMap.get(nextContainerId));

    // test for prev key
    containerMap = reconContainerMetadataManager.getContainers(
        -1, containerId);
    assertEquals(1, containerMap.size());
    // containerId must be skipped from containerMap result
    assertNull(containerMap.get(containerId));

    containerMap = reconContainerMetadataManager.getContainers(
        -1, nextContainerId);
    assertEquals(0, containerMap.size());

    // test for negative cases
    containerMap = reconContainerMetadataManager.getContainers(
        -1, 10L);
    assertEquals(0, containerMap.size());

    containerMap = reconContainerMetadataManager.getContainers(
        0, containerId);
    assertEquals(0, containerMap.size());
  }

  @Test
  public void testDeleteContainerMapping() throws Exception {
    long containerId = 1L;
    long nextContainerId = 2L;
    populateKeysInContainers(containerId, nextContainerId);

    Map<ContainerKeyPrefix, Integer> keyPrefixMap =
        reconContainerMetadataManager.getKeyPrefixesForContainer(containerId);
    assertEquals(2, keyPrefixMap.size());

    keyPrefixMap.forEach((containerKeyPrefix, integer) -> {
      try {
        assertNotNull(reconContainerMetadataManager.getKeyContainerTable()
                          .get(containerKeyPrefix.toKeyPrefixContainer()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    RDBBatchOperation rdbBatchOperation = RDBBatchOperation.newAtomicOperation();
    ContainerKeyPrefix prefixForDelete = ContainerKeyPrefix.get(
        containerId, keyPrefix2, 0);
    reconContainerMetadataManager
        .batchDeleteContainerMapping(rdbBatchOperation, prefixForDelete);
    reconContainerMetadataManager.commitBatchOperation(rdbBatchOperation);
    keyPrefixMap =
        reconContainerMetadataManager.getKeyPrefixesForContainer(containerId);
    assertEquals(1, keyPrefixMap.size());

    assertNull(reconContainerMetadataManager.getKeyContainerTable()
                   .get(prefixForDelete.toKeyPrefixContainer()));
  }

  @Test
  public void testGetCountForContainers() throws Exception {

    assertEquals(0, reconContainerMetadataManager.getCountForContainers());

    reconContainerMetadataManager.storeContainerCount(5L);

    assertEquals(5L, reconContainerMetadataManager.getCountForContainers());
    reconContainerMetadataManager.incrementContainerCountBy(1L);

    assertEquals(6L, reconContainerMetadataManager.getCountForContainers());

    reconContainerMetadataManager.storeContainerCount(10L);
    assertEquals(10L, reconContainerMetadataManager.getCountForContainers());
  }

  @Test
  public void testStoreContainerCount() throws Exception {
    reconContainerMetadataManager.storeContainerCount(3L);
    assertEquals(3L, reconContainerMetadataManager.getCountForContainers());

    reconContainerMetadataManager.storeContainerCount(5L);
    assertEquals(5L, reconContainerMetadataManager.getCountForContainers());
  }

  @Test
  public void testIncrementContainerCountBy() throws Exception {
    assertEquals(0, reconContainerMetadataManager.getCountForContainers());

    reconContainerMetadataManager.incrementContainerCountBy(1L);
    assertEquals(1L, reconContainerMetadataManager.getCountForContainers());

    reconContainerMetadataManager.incrementContainerCountBy(3L);
    assertEquals(4L, reconContainerMetadataManager.getCountForContainers());
  }
}

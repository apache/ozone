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

package org.apache.hadoop.ozone.container.common.impl;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.impl.ContainerStartupCache.CacheMetadata;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Test {@link ContainerStartupCache}.
 */
public class TestContainerStartupCache {
  @TempDir
  private Path tempDir;

  private String scmId = UUID.randomUUID().toString();
  private UUID datanodeId = UUID.randomUUID();
  private VolumeSet volumeSet;
  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;

  @Test
  public void testContainerLoadConsistencyBetweenCacheAndDisk() throws Exception {
    HddsVolume volume = createHddsVolume(randomAlphabetic(10), new OzoneConfiguration());
    List<KeyValueContainer> originalContainers = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      KeyValueContainer container = createTestContainer(i, volume, new OzoneConfiguration());
      container.close();
      originalContainers.add(container);
    }

    Map<Long, KeyValueContainerData> originalContainerData = new HashMap<>();
    for (KeyValueContainer container : originalContainers) {
      KeyValueContainerData data = container.getContainerData();
      originalContainerData.put(data.getContainerID(), data);
    }

    ContainerStartupCache cache = new ContainerStartupCache();
    try {
      cache.dumpContainerCache(originalContainers, volume, new OzoneConfiguration());

      CacheMetadata cacheMetadata = cache.loadContainerCache(volume, new OzoneConfiguration());
      assertNotNull(cacheMetadata);
      assertFalse(cacheMetadata.getContainerYamlData().isEmpty());

      for (Map.Entry<Long, String> entry : cacheMetadata.getContainerYamlData().entrySet()) {
        long containerId = entry.getKey();
        String yamlData = entry.getValue();

        KeyValueContainerData cachedData = (KeyValueContainerData)
            ContainerDataYaml.readContainer(yamlData.getBytes(StandardCharsets.UTF_8));

        KeyValueContainerData originalData = originalContainerData.get(containerId);
        assertNotNull(originalData, "Container should exist in original data");
        KeyValueContainerData fileContainerData = (KeyValueContainerData) ContainerDataYaml
            .readContainerFile(new File(
                originalData.getMetadataPath() + "/" + containerId + ".container"));
        // Compare key container metadata
        assertEquals(fileContainerData.getDataChecksum(), cachedData.getDataChecksum());
        assertEquals(fileContainerData.getChunksPath(), cachedData.getChunksPath());
        assertEquals(fileContainerData.getContainerID(), cachedData.getContainerID());
        assertEquals(fileContainerData.getLayoutVersion(), cachedData.getLayoutVersion());
        assertEquals(fileContainerData.getMaxSize(), cachedData.getMaxSize());
        assertEquals(fileContainerData.getMetadata(), cachedData.getMetadata());
        assertEquals(fileContainerData.getMetadataPath(), cachedData.getMetadataPath());
        assertEquals(fileContainerData.getOriginNodeId(), cachedData.getOriginNodeId());
        assertEquals(fileContainerData.getOriginPipelineId(), cachedData.getOriginPipelineId());
        assertEquals(fileContainerData.getSchemaVersion(), cachedData.getSchemaVersion());
        assertEquals(fileContainerData.getState(), cachedData.getState());
      }
      assertEquals(originalContainers.size(), cacheMetadata.getContainerYamlData().size());

    } finally {
      cache.deleteCacheFile(volume);
    }
  }

  @Test
  public void testMultipleVolumeCaching() throws Exception {
    List<HddsVolume> volumes = new ArrayList<>();
    Map<String, List<KeyValueContainer>> containersByVolume = new HashMap<>();
    List<KeyValueContainer> containers = new ArrayList<>();

    for (int v = 0; v < 3; v++) {
      HddsVolume volume = createHddsVolume(randomAlphabetic(10) + v, new OzoneConfiguration());
      volumes.add(volume);
      List<KeyValueContainer> volumeContainers = new ArrayList<>();
      for (int c = 0; c < 3; c++) {
        long containerId = v * 10 + c;
        KeyValueContainer container = createTestContainer(containerId, volume, new OzoneConfiguration());
        container.close();
        volumeContainers.add(container);
        containers.add(container); // Add to the list of all containers
      }
      containersByVolume.put(volume.getStorageID(), volumeContainers);
    }

    ContainerStartupCache cache = new ContainerStartupCache();
    Map<String, CacheMetadata> volumeCacheData = new HashMap<>();

    try {
      for (HddsVolume volume : volumes) {
        File cacheFile = cache.dumpContainerCache(containers, volume, new OzoneConfiguration());
        assertNotNull(cacheFile);

        CacheMetadata loadedCache = cache.loadContainerCache(volume, new OzoneConfiguration());
        assertNotNull(loadedCache);
        volumeCacheData.put(volume.getStorageID(), loadedCache);
        List<KeyValueContainer> expectedVolumeContainers =
            containersByVolume.get(volume.getStorageID());
        assertEquals(expectedVolumeContainers.size(), loadedCache.getContainerYamlData().size());

        // Verify that only containers belonging to this volume are in the cache
        for (KeyValueContainer expectedContainer : expectedVolumeContainers) {
          long containerId = expectedContainer.getContainerData().getContainerID();
          String cachedYaml = loadedCache.getContainerYamlData().get(containerId);
          assertNotNull(cachedYaml);
          String originalYaml = expectedContainer.getContainerData().getYamlData();
          assertEquals(originalYaml, cachedYaml);
        }

        // Verify that containers from other volumes are NOT in this cache
        for (KeyValueContainer otherContainer : containers) {
          if (!otherContainer.getContainerData().getVolume().getStorageID()
              .equals(volume.getStorageID())) {
            long containerId = otherContainer.getContainerData().getContainerID();
            String cachedYaml = loadedCache.getContainerYamlData().get(containerId);
            assertNull(cachedYaml);
          }
        }
      }

      // Verify total containers across all volumes
      int totalCachedContainers = volumeCacheData.values().stream()
          .mapToInt(metadata -> metadata.getContainerYamlData().size())
          .sum();
      int totalOriginalContainers = containers.size();
      assertEquals(totalOriginalContainers, totalCachedContainers);

    } finally {
      for (HddsVolume volume : volumes) {
        cache.deleteCacheFile(volume);
      }
    }
  }

  @Test
  public void testCacheVersion() throws Exception {
    HddsVolume volume = createHddsVolume(randomAlphabetic(10), new OzoneConfiguration());
    List<KeyValueContainer> testContainers = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      KeyValueContainer container = createTestContainer(i, volume, new OzoneConfiguration());
      container.close();
      testContainers.add(container);
    }

    ContainerStartupCache cache = new ContainerStartupCache();

    try {
      cache.dumpContainerCache(testContainers, volume, new OzoneConfiguration());
      CacheMetadata metadata = cache.loadContainerCache(volume, new OzoneConfiguration());
      assertNotNull(metadata);
      assertEquals(ContainerStartupCache.CacheVersion.CURRENT_VERSION,
          metadata.getVersion());
      assertEquals(ContainerStartupCache.CacheVersion.INITIAL_VERSION,
          ContainerStartupCache.CacheVersion.fromValue(1));
      assertEquals(ContainerStartupCache.CacheVersion.FUTURE_VERSION,
          ContainerStartupCache.CacheVersion.fromValue(999));
      assertNotNull(metadata.getChecksum(), "Checksum should not be null");

    } finally {
      cache.deleteCacheFile(volume);
    }
  }

  @Test
  public void testEmptyContainerCache() throws Exception {
    HddsVolume volume = createHddsVolume(randomAlphabetic(10), new OzoneConfiguration());
    List<KeyValueContainer> emptyContainers = new ArrayList<>();
    ContainerStartupCache cache = new ContainerStartupCache();

    try {
      cache.dumpContainerCache(emptyContainers, volume, new OzoneConfiguration());
      CacheMetadata metadata = cache.loadContainerCache(volume, new OzoneConfiguration());
      assertNotNull(metadata);
      assertEquals(0, metadata.getContainerYamlData().size());

    } finally {
      cache.deleteCacheFile(volume);
    }
  }

  @Test
  public void testLoadContainerCacheFailureScenarios() throws Exception {
    ContainerStartupCache cache = new ContainerStartupCache();
    HddsVolume volume = createHddsVolume(randomAlphabetic(10), new OzoneConfiguration());
    OzoneConfiguration conf = new OzoneConfiguration();

    // Test 1: Cache file does not exist - should return null
    CacheMetadata result = cache.loadContainerCache(volume, conf);
    assertNull(result, "Should return null when cache file does not exist");

    // Create a valid cache first for subsequent tests
    List<KeyValueContainer> containers = new ArrayList<>();
    KeyValueContainer container = createTestContainer(1, volume, conf);
    container.close();
    containers.add(container);
    cache.dumpContainerCache(containers, volume, conf);

    // Test 2: Invalid version - should return null
    File cacheFile = new File(volume.getStorageDir(), "container_cache.json");
    String originalContent = new String(
        Files.readAllBytes(cacheFile.toPath()), StandardCharsets.UTF_8);
    String invalidVersionContent = originalContent.replaceFirst(
        "\"version\":\\d+", "\"version\":999");
    Files.write(cacheFile.toPath(), invalidVersionContent.getBytes(StandardCharsets.UTF_8));
    result = cache.loadContainerCache(volume, conf);
    assertNull(result, "Should return null when cache version is invalid");

    // Test 3: Invalid checksum - should return null
    String invalidChecksumContent = originalContent.replaceFirst(
        "\"checksum\":\"[^\"]+\"", "\"checksum\":\"invalid_checksum\"");
    Files.write(cacheFile.toPath(), invalidChecksumContent.getBytes(StandardCharsets.UTF_8));
    result = cache.loadContainerCache(volume, conf);
    assertNull(result, "Should return null when checksum validation fails");

    // Test 4: Corrupted JSON - should return null
    Files.write(cacheFile.toPath(), "{invalid json".getBytes(StandardCharsets.UTF_8));
    result = cache.loadContainerCache(volume, conf);
    assertNull(result, "Should return null when JSON is corrupted");

    cache.deleteCacheFile(volume);
  }

  private HddsVolume createHddsVolume(String volumeId, OzoneConfiguration conf) throws Exception {
    File volumeDir = Files.createTempDirectory(tempDir, "volume-" + volumeId).toFile();
    HddsVolume volume = new HddsVolume.Builder(volumeDir.getAbsolutePath())
        .conf(conf)
        .datanodeUuid(datanodeId.toString())
        .build();
    StorageVolumeUtil.checkVolume(volume, scmId, scmId, conf, null, null);
    return volume;
  }

  private void setupVolumeSet(List<HddsVolume> hddsVolumes) throws IOException {
    volumeSet = mock(MutableVolumeSet.class);
    volumeChoosingPolicy = mock(RoundRobinVolumeChoosingPolicy.class);

    Mockito.when(volumeSet.getVolumesList())
        .thenAnswer(i -> hddsVolumes.stream()
            .map(v -> (StorageVolume) v)
            .collect(Collectors.toList()));

    Mockito.when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenAnswer(invocation -> {
          List<HddsVolume> volumes = invocation.getArgument(0);
          return volumes.get(0);
        });
  }

  private KeyValueContainer createTestContainer(
      long containerId, HddsVolume volume, OzoneConfiguration conf)
      throws Exception {
    KeyValueContainerData containerData = new KeyValueContainerData(
        containerId,
        ContainerLayoutVersion.FILE_PER_BLOCK,
        (long) StorageUnit.GB.toBytes(5),
        UUID.randomUUID().toString(),
        datanodeId.toString()
    );
    containerData.setSchemaVersion(OzoneConsts.SCHEMA_V3);
    KeyValueContainer container = new KeyValueContainer(containerData, conf);
    List<HddsVolume> volumeList = new ArrayList<>();
    volumeList.add(volume);
    setupVolumeSet(volumeList);
    container.create(volumeSet, volumeChoosingPolicy, scmId);

    return container;
  }

}

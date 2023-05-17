/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.ozone.recon.heatmap;

import com.google.inject.Inject;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.api.handlers.EntityHandler;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityMetaData;
import org.apache.hadoop.ozone.recon.api.types.EntityReadAccessHeatMapResponse;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class is general utility class for keeping heatmap utility functions.
 */
public class HeatMapUtil {
  private static final Logger LOG =
      LoggerFactory.getLogger(HeatMapUtil.class);
  private OzoneConfiguration ozoneConfiguration;
  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private final ReconOMMetadataManager omMetadataManager;
  private final OzoneStorageContainerManager reconSCM;

  @Inject
  public HeatMapUtil(ReconNamespaceSummaryManager
                      namespaceSummaryManager,
                     ReconOMMetadataManager omMetadataManager,
                     OzoneStorageContainerManager reconSCM,
                     OzoneConfiguration ozoneConfiguration) {
    this.reconNamespaceSummaryManager = namespaceSummaryManager;
    this.omMetadataManager = omMetadataManager;
    this.reconSCM = reconSCM;
    this.ozoneConfiguration = ozoneConfiguration;
  }

  private void addBucketData(
      EntityReadAccessHeatMapResponse rootEntity,
      EntityReadAccessHeatMapResponse volumeEntity, String[] split,
      int readAccessCount, long keySize) {
    List<EntityReadAccessHeatMapResponse> children =
        volumeEntity.getChildren();
    EntityReadAccessHeatMapResponse bucketEntity = null;
    List<EntityReadAccessHeatMapResponse> bucketList =
        children.stream().filter(entity -> entity.getLabel().
            equalsIgnoreCase(split[1])).collect(Collectors.toList());
    if (bucketList.size() > 0) {
      bucketEntity = bucketList.get(0);
    }
    if (children.contains(bucketEntity)) {
      addPrefixPathInfoToBucket(rootEntity, split, bucketEntity,
          readAccessCount, keySize);
    } else {
      addBucketAndPrefixPath(split, rootEntity, volumeEntity, readAccessCount,
          keySize);
    }
  }

  private void addVolumeData(
      EntityReadAccessHeatMapResponse rootEntity,
      String[] split, int readAccessCount, long keySize) {
    List<EntityReadAccessHeatMapResponse> children =
        rootEntity.getChildren();
    EntityReadAccessHeatMapResponse volumeInfo =
        new EntityReadAccessHeatMapResponse();
    volumeInfo.setLabel(split[0]);
    children.add(volumeInfo);
    addBucketAndPrefixPath(split, rootEntity, volumeInfo, readAccessCount,
        keySize);
  }

  private void updateVolumeSize(
      EntityReadAccessHeatMapResponse volumeInfo) {
    List<EntityReadAccessHeatMapResponse> children =
        volumeInfo.getChildren();
    children.stream().forEach(bucket -> {
      volumeInfo.setSize(volumeInfo.getSize() + bucket.getSize());
      updateBucketLevelMinMaxAccessCount(bucket);
      updateBucketAccessRatio(bucket);
    });
  }

  private void updateBucketAccessRatio(EntityReadAccessHeatMapResponse bucket) {
    long delta = bucket.getMaxAccessCount() - bucket.getMinAccessCount();
    List<EntityReadAccessHeatMapResponse> children =
        bucket.getChildren();
    children.stream().forEach(path -> {
      path.setColor(1.000);
      if (delta > 0) {
        double truncatedValue = truncate(
            ((double) path.getAccessCount() /
                (double) bucket.getMaxAccessCount()), 3);
        path.setColor(truncatedValue);
      }
    });
  }

  private static double truncate(double value, int decimalPlaces) {
    if (decimalPlaces < 0) {
      throw new IllegalArgumentException();
    }
    value = value * Math.pow(10, decimalPlaces);
    value = Math.floor(value);
    value = value / Math.pow(10, decimalPlaces);
    return value;
  }

  private void updateRootEntitySize(
      EntityReadAccessHeatMapResponse rootEntity) {
    List<EntityReadAccessHeatMapResponse> children =
        rootEntity.getChildren();
    children.stream().forEach(volume -> {
      updateVolumeSize(volume);
      rootEntity.setSize(rootEntity.getSize() + volume.getSize());
    });
  }

  private void addBucketAndPrefixPath(
      String[] split, EntityReadAccessHeatMapResponse rootEntity,
      EntityReadAccessHeatMapResponse volumeEntity,
      long readAccessCount, long keySize) {
    List<EntityReadAccessHeatMapResponse> bucketEntities =
        volumeEntity.getChildren();
    EntityReadAccessHeatMapResponse bucket =
        new EntityReadAccessHeatMapResponse();
    bucket.setLabel(split[1]);
    bucketEntities.add(bucket);
    bucket.setMinAccessCount(readAccessCount);
    addPrefixPathInfoToBucket(rootEntity, split, bucket, readAccessCount,
        keySize);
  }

  private void addPrefixPathInfoToBucket(
      EntityReadAccessHeatMapResponse rootEntity, String[] split,
      EntityReadAccessHeatMapResponse bucket,
      long readAccessCount, long keySize) {
    List<EntityReadAccessHeatMapResponse> prefixes = bucket.getChildren();
    updateBucketSize(bucket, keySize);
    String path = Arrays.stream(split)
        .skip(2).collect(Collectors.joining("/"));
    EntityReadAccessHeatMapResponse prefixPathInfo =
        new EntityReadAccessHeatMapResponse();
    prefixPathInfo.setLabel(path);
    prefixPathInfo.setAccessCount(readAccessCount);
    prefixPathInfo.setSize(keySize);
    prefixes.add(prefixPathInfo);
    // This is done for specific ask by UI treemap to render and provide
    // varying color shades based on varying ranges of access count.
    updateRootLevelMinMaxAccessCount(readAccessCount, rootEntity);
  }

  private void updateBucketLevelMinMaxAccessCount(
      EntityReadAccessHeatMapResponse bucket) {
    List<EntityReadAccessHeatMapResponse> children =
        bucket.getChildren();
    if (children.size() > 0) {
      bucket.setMinAccessCount(Long.MAX_VALUE);
    }
    children.stream().forEach(path -> {
      long readAccessCount = path.getAccessCount();
      bucket.setMinAccessCount(
          path.getAccessCount() < bucket.getMinAccessCount() ? readAccessCount :
              bucket.getMinAccessCount());
      bucket.setMaxAccessCount(
          readAccessCount > bucket.getMaxAccessCount() ? readAccessCount :
              bucket.getMaxAccessCount());
    });
  }

  private void updateRootLevelMinMaxAccessCount(
      long readAccessCount,
      EntityReadAccessHeatMapResponse rootEntity) {
    rootEntity.setMinAccessCount(
        readAccessCount < rootEntity.getMinAccessCount() ? readAccessCount :
            rootEntity.getMinAccessCount());
    rootEntity.setMaxAccessCount(
        readAccessCount > rootEntity.getMaxAccessCount() ? readAccessCount :
            rootEntity.getMaxAccessCount());
  }

  private void updateBucketSize(EntityReadAccessHeatMapResponse bucket,
                                       long keySize) {
    bucket.setSize(bucket.getSize() + keySize);
  }

  public EntityReadAccessHeatMapResponse generateHeatMap(
      EntityMetaData[] entities) {
    EntityReadAccessHeatMapResponse rootEntity =
        new EntityReadAccessHeatMapResponse();
    rootEntity.setMinAccessCount(entities[0].getReadAccessCount());
    rootEntity.setLabel("root");
    List<EntityReadAccessHeatMapResponse> children =
        rootEntity.getChildren();
    Arrays.stream(entities).forEach(entityMetaData -> {
      String path = entityMetaData.getVal();
      String[] split = path.split("/");
      if (split.length == 0) {
        return;
      }
      long keySize = 0;
      try {
        keySize = getEntitySize(path);
      } catch (IOException e) {
        LOG.error("IOException while getting key size for key : " +
            "{} - {}", path, e);
      }
      EntityReadAccessHeatMapResponse volumeEntity = null;
      List<EntityReadAccessHeatMapResponse> volumeList =
          children.stream().filter(entity -> entity.getLabel().
              equalsIgnoreCase(split[0])).collect(Collectors.toList());
      if (volumeList.size() > 0) {
        volumeEntity = volumeList.get(0);
      }
      if (null != volumeEntity) {
        if (validateLength(split, 2)) {
          return;
        }
        addBucketData(rootEntity, volumeEntity, split,
            entityMetaData.getReadAccessCount(), keySize);
      } else {
        if (validateLength(split, 1)) {
          return;
        }
        addVolumeData(rootEntity, split,
            entityMetaData.getReadAccessCount(), keySize);
      }
    });
    updateRootEntitySize(rootEntity);
    return rootEntity;
  }

  private static boolean validateLength(String[] split, int minLength) {
    return (split.length < minLength);
  }

  private long getEntitySize(String path) throws IOException {
    long entitySize = 0;
    LOG.info("Getting entity size for {}: ", path);
    EntityHandler entityHandler =
        EntityHandler.getEntityHandler(reconNamespaceSummaryManager,
            omMetadataManager, reconSCM, path);
    if (null != entityHandler) {
      DUResponse duResponse = entityHandler.getDuResponse(false, false);
      if (null != duResponse && duResponse.getStatus() == ResponseStatus.OK) {
        return duResponse.getSize();
      }
    }
    // returning some default value due to some issue
    return 256L;
  }

  public EntityReadAccessHeatMapResponse retrieveData(
      IHeatMapProvider heatMapProvider, String normalizePath,
      String entityType,
      String startDate) throws Exception {
    if (null != heatMapProvider) {
      EntityMetaData[] entities = heatMapProvider.retrieveData(normalizePath,
          entityType, startDate);
      if (null != entities && !(ArrayUtils.isEmpty(entities))) {
        return generateHeatMap(entities);
      }
    }
    return null;
  }
}

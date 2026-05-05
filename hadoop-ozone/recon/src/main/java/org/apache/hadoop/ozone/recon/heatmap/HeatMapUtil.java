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

package org.apache.hadoop.ozone.recon.heatmap;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

import com.google.inject.Inject;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
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

/**
 * This class is general utility class for keeping heatmap utility functions.
 */
public class HeatMapUtil {
  private static final Logger LOG =
      LoggerFactory.getLogger(HeatMapUtil.class);
  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private final ReconOMMetadataManager omMetadataManager;
  private final OzoneStorageContainerManager reconSCM;

  @Inject
  public HeatMapUtil(ReconNamespaceSummaryManager
                      namespaceSummaryManager,
                     ReconOMMetadataManager omMetadataManager,
                     OzoneStorageContainerManager reconSCM) {
    this.reconNamespaceSummaryManager = namespaceSummaryManager;
    this.omMetadataManager = omMetadataManager;
    this.reconSCM = reconSCM;
  }

  private long getEntitySize(String path) throws IOException {
    long entitySize = 0;
    LOG.info("Getting entity size for {}: ", path);
    EntityHandler entityHandler =
        EntityHandler.getEntityHandler(reconNamespaceSummaryManager,
            omMetadataManager, reconSCM, path);
    if (null != entityHandler) {
      DUResponse duResponse = entityHandler.getDuResponse(false, false, false);
      if (null != duResponse && duResponse.getStatus() == ResponseStatus.OK) {
        return duResponse.getSize();
      }
    }
    // returning some default value due to some issue
    return 256L;
  }

  private static boolean validateLength(String[] split, int minLength) {
    return (split.length < minLength);
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
    if (!bucketList.isEmpty()) {
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
    prefixPathInfo.setPath(bucket.getPath() + OM_KEY_PREFIX + path);
    prefixPathInfo.setAccessCount(readAccessCount);
    prefixPathInfo.setSize(keySize);
    prefixes.add(prefixPathInfo);
    // This is done for specific ask by UI treemap to render and provide
    // varying color shades based on varying ranges of access count.
    updateRootLevelMinMaxAccessCount(readAccessCount, rootEntity);
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
    bucket.setPath(omMetadataManager.getBucketKey(split[0], split[1]));
    bucketEntities.add(bucket);
    bucket.setMinAccessCount(readAccessCount);
    if (split.length > 2) {
      addPrefixPathInfoToBucket(rootEntity, split, bucket, readAccessCount,
          keySize);
    } else {
      updateBucketSize(bucket, keySize);
    }
  }

  private void addVolumeData(
      EntityReadAccessHeatMapResponse rootEntity,
      String[] split, int readAccessCount, long entitySize) {
    List<EntityReadAccessHeatMapResponse> children =
        rootEntity.getChildren();
    EntityReadAccessHeatMapResponse volumeInfo =
        new EntityReadAccessHeatMapResponse();
    volumeInfo.setLabel(split[0]);
    volumeInfo.setPath(split[0]);
    children.add(volumeInfo);
    if (split.length < 2) {
      volumeInfo.setSize(entitySize);
      volumeInfo.setAccessCount(readAccessCount);
      volumeInfo.setMinAccessCount(readAccessCount);
      volumeInfo.setMaxAccessCount(readAccessCount);
      return;
    }
    addBucketAndPrefixPath(split, rootEntity, volumeInfo, readAccessCount,
        entitySize);
  }

  private void setEntityLevelAccessCount(
      EntityReadAccessHeatMapResponse entity) {
    List<EntityReadAccessHeatMapResponse> children = entity.getChildren();
    children.stream().forEach(child -> {
      entity.setAccessCount(entity.getAccessCount() + child.getAccessCount());
    });
    // This is being taken as whole number
    if (entity.getAccessCount() > 0 && !children.isEmpty()) {
      entity.setAccessCount(entity.getAccessCount() / children.size());
    }
  }

  private void updateBucketLevelMinMaxAccessCount(
      EntityReadAccessHeatMapResponse bucket) {
    List<EntityReadAccessHeatMapResponse>
        children = initializeEntityMinMaxCount(bucket);
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

  private void updateVolumeSize(
      EntityReadAccessHeatMapResponse volumeInfo) {
    List<EntityReadAccessHeatMapResponse> children =
        volumeInfo.getChildren();
    children.stream().forEach(bucket -> {
      volumeInfo.setSize(volumeInfo.getSize() + bucket.getSize());
      updateBucketLevelMinMaxAccessCount(bucket);
    });
  }

  private void updateRootEntitySize(
      EntityReadAccessHeatMapResponse rootEntity) {
    List<EntityReadAccessHeatMapResponse> children =
        rootEntity.getChildren();
    children.stream().forEach(volume -> {
      updateVolumeSize(volume);
      updateVolumeLevelMinMaxAccessCount(volume);
      setEntityLevelAccessCount(volume);
      rootEntity.setSize(rootEntity.getSize() + volume.getSize());
    });
  }

  @Nonnull
  private static List<EntityReadAccessHeatMapResponse>
      initializeEntityMinMaxCount(
      EntityReadAccessHeatMapResponse entity) {
    List<EntityReadAccessHeatMapResponse> children =
        entity.getChildren();
    if (children.isEmpty()) {
      entity.setMaxAccessCount(entity.getMinAccessCount());
    }
    if (!children.isEmpty()) {
      entity.setMinAccessCount(Long.MAX_VALUE);
    }
    return children;
  }

  private void updateVolumeLevelMinMaxAccessCount(
      EntityReadAccessHeatMapResponse volume) {
    List<EntityReadAccessHeatMapResponse>
        children = initializeEntityMinMaxCount(volume);
    children.stream().forEach(child -> {
      long bucketMinAccessCount = child.getMinAccessCount();
      long bucketMaxAccessCount = child.getMaxAccessCount();
      volume.setMinAccessCount(
          bucketMinAccessCount < volume.getMinAccessCount() ?
              bucketMinAccessCount :
              volume.getMinAccessCount());
      volume.setMaxAccessCount(
          bucketMaxAccessCount > volume.getMaxAccessCount() ?
              bucketMaxAccessCount :
              volume.getMaxAccessCount());
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

  private void updateEntityAccessRatio(EntityReadAccessHeatMapResponse entity) {
    long delta = entity.getMaxAccessCount() - entity.getMinAccessCount();
    List<EntityReadAccessHeatMapResponse> children =
        entity.getChildren();
    children.stream().forEach(path -> {
      if (!path.getChildren().isEmpty()) {
        updateEntityAccessRatio(path);
      } else {
        path.setColor(1.000);
        long accessCount = path.getAccessCount();
        accessCount =
            (accessCount == 0 ? path.getMinAccessCount() : accessCount);
        if (delta >= 0) {
          if (accessCount > 0) {
            double truncatedValue = truncate(
                ((double) accessCount /
                    (double) entity.getMaxAccessCount()), 3);
            path.setColor(truncatedValue);
          }
        }
      }
    });
  }

  /**
   * Transforms the access metadata of entities (provided by heatmap provider)
   * by grouping them in their respective buckets and volumes  in tree structure
   * format which is easy for heatmap UI to consume.
   * Sample Data Format:
   * {
   *   "label": "root",
   *   "children": [
   *     {
   *       "label": "hivevol1676574631",
   *       "children": [
   *         {
   *           "label": "hiveencbuck1676574631",
   *           "children": [
   *             {
   *               "label": "enc_path/hive_tpcds/store_sales/store_sales.dat",
   *               "size": 256,
   *               "path": "/hivevol1676574631/hiveencbuck1676574631/enc_path/
   *               hive_tpcds/store_sales/store_sales.dat",
   *               "accessCount": 155074,
   *               "color": 1
   *             },
   *             {
   *               "label": "enc_path/hive_tpcds/catalog_sales/
   *               catalog_sales.dat",
   *               "size": 256,
   *               "path": "/hivevol1676574631/hiveencbuck1676574631/enc_path/
   *               hive_tpcds/catalog_sales/catalog_sales.dat",
   *               "accessCount": 68567,
   *               "color": 0.442
   *             }
   *           ],
   *           "size": 3584,
   *           "path": "/hivevol1676574631/hiveencbuck1676574631",
   *           "minAccessCount": 2924,
   *           "maxAccessCount": 155074
   *         },
   *         {
   *           "label": "hivebuck1676574631",
   *           "children": [
   *             {
   *               "label": "reg_path/hive_tpcds/store_sales/store_sales.dat",
   *               "size": 256,
   *               "path": "/hivevol1676574631/hivebuck1676574631/reg_path/
   *               hive_tpcds/store_sales/store_sales.dat",
   *               "accessCount": 155069,
   *               "color": 1
   *             },
   *             {
   *               "label": "reg_path/hive_tpcds/catalog_sales/
   *               catalog_sales.dat",
   *               "size": 256,
   *               "path": "/hivevol1676574631/hivebuck1676574631/reg_path/
   *               hive_tpcds/catalog_sales/catalog_sales.dat",
   *               "accessCount": 68566,
   *               "color": 0.442
   *             }
   *           ],
   *           "size": 3584,
   *           "path": "/hivevol1676574631/hivebuck1676574631",
   *           "minAccessCount": 2924,
   *           "maxAccessCount": 155069
   *         }
   *       ],
   *       "size": 7168,
   *       "path": "/hivevol1676574631"
   *     },
   *     {
   *       "label": "hivevol1675429570",
   *       "children": [
   *         {
   *           "label": "hivebuck1675429570",
   *           "children": [
   *             {
   *               "label": "reg_path/hive_tpcds/store_sales/store_sales.dat",
   *               "size": 256,
   *               "path": "/hivevol1675429570/hivebuck1675429570/reg_path/
   *               hive_tpcds/store_sales/store_sales.dat",
   *               "accessCount": 129977,
   *               "color": 1
   *             }          ],
   *           "size": 3072,
   *           "path": "/hivevol1675429570/hivebuck1675429570",
   *           "minAccessCount": 3195,
   *           "maxAccessCount": 129977
   *         }
   *       ],
   *       "size": 6144,
   *       "path": "/hivevol1675429570"
   *     }
   *   ],
   *   "size": 25600,
   *   "minAccessCount": 2924,
   *   "maxAccessCount": 155074
   * }
   * @param entityMetaDataList list of entity's access metadata objects
   * @return transformed data to be consumed by heatmap UI.
   */
  public EntityReadAccessHeatMapResponse generateHeatMap(
      List<EntityMetaData> entityMetaDataList) {
    EntityReadAccessHeatMapResponse rootEntity =
        new EntityReadAccessHeatMapResponse();
    rootEntity.setMinAccessCount(
        entityMetaDataList.get(0).getReadAccessCount());
    rootEntity.setLabel("root");
    rootEntity.setPath(OM_KEY_PREFIX);
    List<EntityReadAccessHeatMapResponse> children =
        rootEntity.getChildren();
    entityMetaDataList.forEach(entityMetaData -> {
      String path = entityMetaData.getVal();
      String[] split = path.split("/");
      if (split.length == 0) {
        return;
      }
      long entitySize = 0;
      try {
        entitySize = getEntitySize(path);
      } catch (IOException e) {
        LOG.error("IOException while getting key size for key : " +
            "{} - {}", path, e);
      }
      EntityReadAccessHeatMapResponse volumeEntity = null;
      List<EntityReadAccessHeatMapResponse> volumeList =
          children.stream().filter(entity -> entity.getLabel().
              equalsIgnoreCase(split[0])).collect(Collectors.toList());
      if (!volumeList.isEmpty()) {
        volumeEntity = volumeList.get(0);
      }
      if (null != volumeEntity) {
        if (validateLength(split, 2)) {
          return;
        }
        addBucketData(rootEntity, volumeEntity, split,
            entityMetaData.getReadAccessCount(), entitySize);
      } else {
        if (validateLength(split, 1)) {
          return;
        }
        addVolumeData(rootEntity, split,
            entityMetaData.getReadAccessCount(), entitySize);
      }
    });
    updateRootEntitySize(rootEntity);
    updateVolumeLevelMinMaxAccessCount(rootEntity);
    updateEntityAccessRatio(rootEntity);
    return rootEntity;
  }

  public EntityReadAccessHeatMapResponse retrieveDataAndGenerateHeatMap(
      IHeatMapProvider heatMapProvider, String normalizePath,
      String entityType,
      String startDate) throws Exception {
    if (null != heatMapProvider) {
      List<EntityMetaData> entityMetaDataList = heatMapProvider
          .retrieveData(normalizePath, entityType, startDate);
      if (null != entityMetaDataList &&
          (CollectionUtils.isNotEmpty(entityMetaDataList))) {
        // Transforms and return heatmap data by grouping access metadata of
        // entities in their respective buckets and volumes in tree structure.
        // Refer the javadoc for more details.ß
        return generateHeatMap(entityMetaDataList);
      }
    }
    return new EntityReadAccessHeatMapResponse();
  }

  /**
   * This method loads heatMapProvider implementation class.
   *
   * @param className - load the class and instantiate object.
   * @return the implementation class object of IHeatMapProvider
   * @throws Exception
   */
  public static IHeatMapProvider loadHeatMapProvider(String className)
      throws Exception {
    try {
      Class<?> clazz = Class.forName(className);
      Object o = clazz.newInstance();
      if (o instanceof IHeatMapProvider) {
        return (IHeatMapProvider) o;
      }
      return null;
    } catch (ClassNotFoundException | InstantiationException |
             IllegalAccessException e) {
      throw new Exception(e);
    }
  }
}

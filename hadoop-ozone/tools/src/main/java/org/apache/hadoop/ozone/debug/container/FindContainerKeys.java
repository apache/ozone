/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.debug.container;

import com.google.common.collect.Sets;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.debug.OzoneDebug;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.ROOT_PATH;

/**
 * Finds keys that reference a container/s.
 */
@CommandLine.Command(
    name = "find-keys",
    description = "Find keys that reference a container"
)
@MetaInfServices(SubcommandWithParent.class)
public class FindContainerKeys
    implements Callable<Void>, SubcommandWithParent {

  public static final Logger LOG =
      LoggerFactory.getLogger(FindContainerKeys.class);
  @CommandLine.Spec
  private static CommandLine.Model.CommandSpec spec;
  @CommandLine.Option(names = {"--om-db"},
      paramLabel = "<OM DB path>",
      required = true,
      description = "Path to OM DB.")
  private String dbPath;
  @CommandLine.Option(names = {"--container-ids"},
      split = ",",
      paramLabel = "<container ID>",
      required = true,
      description = "One or more container IDs separated by comma.")
  private Set<Long> containerIds;
  private static Map<String, OmDirectoryInfo> directoryTable;
  private static boolean isDirTableLoaded = false;

  @Override
  public Void call() throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set("ozone.om.db.dirs",
        dbPath.substring(0, dbPath.lastIndexOf("/")));
    OmMetadataManagerImpl omMetadataManager =
        new OmMetadataManagerImpl(ozoneConfiguration, null);

    ContainerKeyInfoResponse containerKeyInfoResponse =
        scanDBForContainerKeys(omMetadataManager);

    printOutput(containerKeyInfoResponse);

    closeStdChannels();

    return null;
  }

  private void closeStdChannels() {
    out().close();
    err().close();
  }

  private Map<String, OmDirectoryInfo> getDirectoryTableData(
      OmMetadataManagerImpl metadataManager)
      throws IOException {
    Map<String, OmDirectoryInfo> directoryTableData = new HashMap<>();

    try (
        TableIterator<String, ? extends Table.KeyValue<String, OmDirectoryInfo>>
            iterator = metadataManager.getDirectoryTable().iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmDirectoryInfo> next = iterator.next();
        directoryTableData.put(next.getKey(), next.getValue());
      }
    }

    return directoryTableData;
  }

  @Override
  public Class<?> getParentType() {
    return OzoneDebug.class;
  }

  private static PrintWriter err() {
    return spec.commandLine().getErr();
  }

  private static PrintWriter out() {
    return spec.commandLine().getOut();
  }

  public Map<Long, Path> getAbsolutePathForObjectIDs(
      long bucketId, String prefix, Optional<Set<Long>> dirObjIds) {
    // Root of a bucket would always have the
    // key as /volumeId/bucketId/bucketId/
    if (!dirObjIds.isPresent() || dirObjIds.get().isEmpty()) {
      return Collections.emptyMap();
    }
    Set<Long> objIds = Sets.newHashSet(dirObjIds.get());
    Map<Long, Path> objectIdPathMap = new HashMap<>();
    Queue<Pair<Long, Path>> objectIdPathVals = new LinkedList<>();
    Pair<Long, Path> root = Pair.of(bucketId, ROOT_PATH);
    objectIdPathVals.add(root);
    addToPathMap(root, objIds, objectIdPathMap);

    while (!objectIdPathVals.isEmpty() && !objIds.isEmpty()) {
      Pair<Long, Path> parentPair = objectIdPathVals.poll();
      String subDir = prefix + parentPair.getKey() + OM_KEY_PREFIX;

      Iterator<String> subDirIterator =
          directoryTable.keySet().stream()
              .filter(k -> k.startsWith(subDir))
              .collect(Collectors.toList()).iterator();
      while (!objIds.isEmpty() && subDirIterator.hasNext()) {
        OmDirectoryInfo childDir =
            directoryTable.get(subDirIterator.next());
        Pair<Long, Path> pathVal = Pair.of(childDir.getObjectID(),
            parentPair.getValue().resolve(childDir.getName()));
        addToPathMap(pathVal, objIds, objectIdPathMap);
        objectIdPathVals.add(pathVal);
      }
    }
    // Invalid directory objectId which does not exist in the given bucket.
    if (!objIds.isEmpty()) {
      throw new IllegalArgumentException(
          "Dir object Ids required but not found in bucket: " + objIds);
    }
    return objectIdPathMap;
  }

  private void addToPathMap(Pair<Long, Path> objectIDPath,
                            Set<Long> dirObjIds, Map<Long, Path> pathMap) {
    if (dirObjIds.contains(objectIDPath.getKey())) {
      pathMap.put(objectIDPath.getKey(), objectIDPath.getValue());
      dirObjIds.remove(objectIDPath.getKey());
    }
  }

  private ContainerKeyInfoResponse scanDBForContainerKeys(
      OmMetadataManagerImpl omMetadataManager)
      throws IOException {
    Map<Long, List<ContainerKeyInfo>> containerKeyInfos = new HashMap<>();

    long keysProcessed = 0;

    keysProcessed += processFileTable(containerKeyInfos, omMetadataManager);
    keysProcessed += processKeyTable(containerKeyInfos, omMetadataManager);

    return new ContainerKeyInfoResponse(keysProcessed, containerKeyInfos);
  }

  private long processKeyTable(
      Map<Long, List<ContainerKeyInfo>> containerKeyInfos,
      OmMetadataManagerImpl omMetadataManager) throws IOException {
    long keysProcessed = 0L;

    // Anything but not FSO bucket layout
    Table<String, OmKeyInfo> fileTable = omMetadataManager.getKeyTable(
        BucketLayout.DEFAULT);
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
             iterator = fileTable.iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> next = iterator.next();
        keysProcessed++;

        if (Objects.isNull(next.getValue().getKeyLocationVersions())) {
          continue;
        }

        processKeyData(containerKeyInfos, next.getKey(), next.getValue());
      }
    }

    return keysProcessed;
  }


  private long processFileTable(
      Map<Long, List<ContainerKeyInfo>> containerKeyInfos,
      OmMetadataManagerImpl omMetadataManager)
      throws IOException {
    long keysProcessed = 0L;

    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
             iterator = omMetadataManager.getFileTable().iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> next = iterator.next();
        keysProcessed++;

        if (Objects.isNull(next.getValue().getKeyLocationVersions())) {
          continue;
        }

        processFileData(containerKeyInfos, next.getKey(), next.getValue(),
            omMetadataManager);
      }
    }

    return keysProcessed;
  }

  /**
   * @param key file table key.
   * @return Pair of volume id and bucket id.
   */
  private Pair<Long, Long> parseKey(String key) {
    String[] keyParts = key.split(OM_KEY_PREFIX);
    return Pair.of(Long.parseLong(keyParts[1]), Long.parseLong(keyParts[2]));
  }

  private void processKeyData(
      Map<Long, List<ContainerKeyInfo>> containerKeyInfos,
      String key, OmKeyInfo keyInfo) {
    long volumeId = 0L;
    long bucketId = 0L;

    for (OmKeyLocationInfoGroup locationInfoGroup :
        keyInfo.getKeyLocationVersions()) {
      for (List<OmKeyLocationInfo> locationInfos :
          locationInfoGroup.getLocationVersionMap().values()) {
        for (OmKeyLocationInfo locationInfo : locationInfos) {
          if (containerIds.contains(locationInfo.getContainerID())) {

            containerKeyInfos.merge(locationInfo.getContainerID(),
                new ArrayList<>(Collections.singletonList(
                    new ContainerKeyInfo(locationInfo.getContainerID(),
                        keyInfo.getVolumeName(), volumeId,
                        keyInfo.getBucketName(), bucketId, keyInfo.getKeyName(),
                        keyInfo.getParentObjectID()))),
                (existingList, newList) -> {
                  existingList.addAll(newList);
                  return existingList;
                });
          }
        }
      }
    }
  }

  private void processFileData(
      Map<Long, List<ContainerKeyInfo>> containerKeyInfos,
      String key, OmKeyInfo keyInfo, OmMetadataManagerImpl omMetadataManager)
      throws IOException {

    Pair<Long, Long> volumeAndBucketId = parseKey(key);
    Long volumeId = volumeAndBucketId.getLeft();
    Long bucketId = volumeAndBucketId.getRight();

    for (OmKeyLocationInfoGroup locationInfoGroup :
        keyInfo.getKeyLocationVersions()) {
      for (List<OmKeyLocationInfo> locationInfos :
          locationInfoGroup.getLocationVersionMap().values()) {
        for (OmKeyLocationInfo locationInfo : locationInfos) {
          if (containerIds.contains(locationInfo.getContainerID())) {
            StringBuilder keyName = new StringBuilder();
            if (!isDirTableLoaded) {
              long start = System.currentTimeMillis();
              directoryTable = getDirectoryTableData(omMetadataManager);
              long end = System.currentTimeMillis();
              LOG.info("directoryTable loaded in " + (end - start) + " ms.");
              isDirTableLoaded = true;
            }
            keyName.append(getFsoKeyPrefix(volumeId, bucketId, keyInfo));
            keyName.append(keyInfo.getKeyName());

            containerKeyInfos.merge(locationInfo.getContainerID(),
                new ArrayList<>(Collections.singletonList(
                    new ContainerKeyInfo(locationInfo.getContainerID(),
                        keyInfo.getVolumeName(), volumeId,
                        keyInfo.getBucketName(), bucketId, keyName.toString(),
                        keyInfo.getParentObjectID()))),
                (existingList, newList) -> {
                  existingList.addAll(newList);
                  return existingList;
                });
          }
        }
      }
    }
  }

  private static String removeBeginningSlash(String path) {
    if (path.startsWith(OM_KEY_PREFIX)) {
      return path.substring(1);
    }

    return path;
  }

  private String getFsoKeyPrefix(long volumeId, long bucketId,
                                 OmKeyInfo value) {
    String prefix =
        OM_KEY_PREFIX + volumeId + OM_KEY_PREFIX + bucketId +
            OM_KEY_PREFIX;
    Set<Long> dirObjIds = new HashSet<>();
    dirObjIds.add(value.getParentObjectID());
    Map<Long, Path> absolutePaths =
        getAbsolutePathForObjectIDs(bucketId, prefix, Optional.of(dirObjIds));
    Path path = absolutePaths.get(value.getParentObjectID());
    String keyPath;
    if (path.toString().equals(OM_KEY_PREFIX)) {
      keyPath = path.toString();
    } else {
      keyPath = path + OM_KEY_PREFIX;
    }

    return removeBeginningSlash(keyPath);
  }

  private void printOutput(ContainerKeyInfoResponse containerKeyInfoResponse) {
    if (containerKeyInfoResponse.getContainerKeys().isEmpty()) {
      err().println("No keys were found for container IDs: " + containerIds);
      err().println(
          "Keys processed: " + containerKeyInfoResponse.getKeysProcessed());
      return;
    }

    out().print(new GsonBuilder().setPrettyPrinting().create()
        .toJson(containerKeyInfoResponse));
  }

}

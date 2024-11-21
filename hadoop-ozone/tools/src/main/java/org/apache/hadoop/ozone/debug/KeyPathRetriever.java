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

package org.apache.hadoop.ozone.debug;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Tool that retrieve key full path from OM db file table with pattern match.
 */
@CommandLine.Command(
    name = "retrieve-key-fullpath",
    description = "retrieve full key path with matching criteria")
@MetaInfServices(SubcommandWithParent.class)
public class KeyPathRetriever implements Callable<Void>, SubcommandWithParent {
  @CommandLine.Spec
  private static CommandLine.Model.CommandSpec spec;

  @CommandLine.Option(names = {"--db"},
      required = true,
      description = "Database File")
  private String dbFile;

  @CommandLine.Option(names = {"--containers"},
      required = false,
      description = "Comma separated Container Ids")
  private String strContainerIds;

  @CommandLine.Option(names = {"--container-file"},
      required = false,
      description = "file with container id in new line")
  private String containerFileName;

  @CommandLine.Option(names = {"--out", "-o"},
      required = false,
      description = "out file name")
  private String fileName;

  @Override
  public Class<?> getParentType() {
    return OzoneDebug.class;
  }

  @Override
  public Void call() throws Exception {
    // get containerIds filter
    Set<Long> containerIds = new HashSet<>();
    if (!StringUtils.isEmpty(strContainerIds)) {
      if (!StringUtils.isEmpty(strContainerIds)) {
        String[] split = strContainerIds.split(",");
        for (String id : split) {
          containerIds.add(Long.parseLong(id));
        }
      }
    } else if (!StringUtils.isEmpty(containerFileName)) {
      try (Stream<String> stream = Files.lines(Paths.get(containerFileName))) {
        stream.forEach(e -> containerIds.add(Long.parseLong(e)));
      }
    }
    if (containerIds.isEmpty()) {
      System.out.println("No containers to be filtered");
      return null;
    }
    // out stream
    PrintWriter writer;
    if (StringUtils.isEmpty(fileName)) {
      writer = out();
    } else {
      writer = new PrintWriter(new BufferedWriter(new PrintWriter(fileName, UTF_8.name())));
    }

    // db handler
    OMMetadataManager metadataManager = getOmMetadataManager(dbFile);
    if (metadataManager == null) {
      writer.close();
      return null;
    }

    try {
      retrieve(metadataManager, writer, containerIds);
    } finally {
      writer.close();
      metadataManager.stop();
    }
    return null;
  }

  public void retrieve(
      OMMetadataManager metadataManager, PrintWriter writer, Set<Long> containerIds) {
    // build dir tree
    Map<Long, Pair<Long, String>> dirObjNameParentMap = new HashMap<>();
    try {
      prepareDirIdTree(metadataManager, dirObjNameParentMap);
    } catch (Exception e) {
      System.out.println("Exception occurred reading directory Table, " + e);
      return;
    }

    // iterate file table and filter for container
    try (TableIterator<String, ? extends KeyValue<String, OmKeyInfo>> fileItr
            = metadataManager.getFileTable().iterator()) {
      while (fileItr.hasNext()) {
        KeyValue<String, OmKeyInfo> next = fileItr.next();
        boolean found = next.getValue().getKeyLocationVersions().stream().anyMatch(
            e -> e.getLocationList().stream().anyMatch(
                blk -> containerIds.contains(blk.getBlockID().getContainerID())));
        if (found) {
          StringBuilder sb = new StringBuilder(next.getValue().getKeyName());
          Long prvParent = next.getValue().getParentObjectID();
          while (dirObjNameParentMap.containsKey(prvParent)) {
            Pair<Long, String> nameParentPair = dirObjNameParentMap.get(prvParent);
            sb.insert(0, nameParentPair.getValue() + OM_KEY_PREFIX);
            prvParent = nameParentPair.getKey();
            if (null == prvParent) {
              // add to output as reached till volume
              writer.println(sb);
              break;
            }
          }
        }
      }
    } catch (Exception e) {
      System.out.println("Exception occurred reading file Table, " + e);
    }
  }

  private static OMMetadataManager getOmMetadataManager(String db) throws IOException {
    if (!Files.exists(Paths.get(db))) {
      System.out.println("DB with path not exist:" + db);
      return null;
    }
    System.out.println("Db Path is:" + db);
    File file = new File(db);

    OzoneConfiguration conf = new OzoneConfiguration();
    return new OmMetadataManagerImpl(conf, file.getParentFile(), file.getName());
  }

  private static void prepareDirIdTree(
      OMMetadataManager metadataManager, Map<Long, Pair<Long, String>> dirObjNameParentMap) throws IOException {
    // add bucket volume tree
    try (TableIterator<String, ? extends KeyValue<String, OmBucketInfo>> bucItr
             = metadataManager.getBucketTable().iterator()) {
      while (bucItr.hasNext()) {
        KeyValue<String, OmBucketInfo> next = bucItr.next();
        dirObjNameParentMap.put(next.getValue().getObjectID(),
            Pair.of(metadataManager.getVolumeId(next.getValue().getVolumeName()), next.getValue().getBucketName()));
        dirObjNameParentMap.putIfAbsent(metadataManager.getVolumeId(next.getValue().getVolumeName()),
            Pair.of(null, next.getValue().getVolumeName()));
      }
    }
    // add dir tree
    try (TableIterator<String, ? extends KeyValue<String, OmDirectoryInfo>> dirItr
             = metadataManager.getDirectoryTable().iterator()) {
      while (dirItr.hasNext()) {
        KeyValue<String, OmDirectoryInfo> next = dirItr.next();
        dirObjNameParentMap.put(next.getValue().getObjectID(), Pair.of(next.getValue().getParentObjectID(),
            next.getValue().getName()));
      }
    }
  }

  private static PrintWriter out() {
    return spec.commandLine().getOut();
  }
}

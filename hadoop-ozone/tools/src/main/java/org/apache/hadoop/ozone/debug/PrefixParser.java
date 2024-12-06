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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import java.nio.file.Path;
import org.apache.hadoop.hdds.cli.DebugSubcommand;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManagerUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.WithParentObjectId;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Tool that parses OM db file for prefix table.
 */
@CommandLine.Command(
    name = "prefix",
    description = "Parse prefix contents")
@MetaInfServices(DebugSubcommand.class)
public class PrefixParser implements Callable<Void>, DebugSubcommand {

  /**
   * Types to represent the level or path component type.
   */
  public enum Types {
    VOLUME,
    BUCKET,
    FILE,
    DIRECTORY,
    INTERMEDIATE_DIRECTORY,
    NON_EXISTENT_DIRECTORY,
  }

  private final int[] parserStats = new int[Types.values().length];

  @Spec
  private CommandSpec spec;

  @CommandLine.Option(names = {"--db"},
      required = true,
      description = "Database File Path")
  private String dbPath;

  @CommandLine.Option(names = {"--path"},
      required = true,
      description = "prefixFile Path")
  private String filePath;

  @CommandLine.Option(names = {"--bucket"},
      required = true,
      description = "bucket name")
  private String bucket;

  @CommandLine.Option(names = {"--volume"},
      required = true,
      description = "volume name")
  private String volume;

  public String getDbPath() {
    return dbPath;
  }

  public void setDbPath(String dbPath) {
    this.dbPath = dbPath;
  }

  @Override
  public Void call() throws Exception {
    parse(volume, bucket, dbPath, filePath);
    return null;
  }

  public static void main(String[] args) throws Exception {
    new PrefixParser().call();
  }

  public void parse(String vol, String buck, String db,
                    String file) throws Exception {
    if (!Files.exists(Paths.get(db))) {
      System.out.println("DB path not exist:" + db);
      return;
    }

    System.out.println("FilePath is:" + file);
    System.out.println("Db Path is:" + db);

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, db);

    OmMetadataManagerImpl metadataManager =
        new OmMetadataManagerImpl(conf, null);
    metadataManager.start(conf);

    org.apache.hadoop.fs.Path effectivePath =
        new org.apache.hadoop.fs.Path("/");

    Path p = Paths.get(file);

    String volumeKey = metadataManager.getVolumeKey(vol);
    if (!metadataManager.getVolumeTable().isExist(volumeKey)) {
      System.out.println("Invalid Volume:" + vol);
      metadataManager.stop();
      return;
    }

    parserStats[Types.VOLUME.ordinal()]++;
    // First get the info about the bucket
    OmBucketInfo info;
    try {
      info = OzoneManagerUtils
          .getResolvedBucketInfo(metadataManager, vol, buck);
    } catch (OMException e) {
      System.out.println("Invalid Bucket:" + buck);
      metadataManager.stop();
      return;
    }
    BucketLayout bucketLayout = info.getBucketLayout();

    if (!bucketLayout.isFileSystemOptimized()) {
      System.out.println("Prefix tool only works for FileSystem Optimized" +
              "bucket. Bucket Layout is:" + bucketLayout);
      metadataManager.stop();
      return;
    }

    final long volumeObjectId = metadataManager.getVolumeId(
            info.getVolumeName());
    long lastObjectId = info.getObjectID();
    WithParentObjectId objectBucketId = new WithParentObjectId();
    objectBucketId.setObjectID(lastObjectId);
    dumpInfo(Types.BUCKET, effectivePath, objectBucketId,
        metadataManager.getBucketKey(vol, buck));

    Iterator<Path> pathIterator =  p.iterator();
    while (pathIterator.hasNext()) {
      Path elem = pathIterator.next();
      String path =
          metadataManager.getOzonePathKey(volumeObjectId, info.getObjectID(),
                  lastObjectId, elem.toString());
      OmDirectoryInfo directoryInfo =
          metadataManager.getDirectoryTable().get(path);

      org.apache.hadoop.fs.Path tmpPath =
          getEffectivePath(effectivePath, elem.toString());
      if (directoryInfo == null) {
        System.out.println("Given path contains a non-existent directory at:" +
            tmpPath);
        System.out.println("Dumping files and dirs at level:" +
            tmpPath.getParent());
        System.out.println();
        parserStats[Types.NON_EXISTENT_DIRECTORY.ordinal()]++;
        break;
      }

      effectivePath = tmpPath;

      dumpInfo(Types.INTERMEDIATE_DIRECTORY, effectivePath,
          directoryInfo, path);
      lastObjectId = directoryInfo.getObjectID();
    }

    // at the last level, now parse both file and dir table
    dumpTableInfo(Types.DIRECTORY, effectivePath,
        metadataManager.getDirectoryTable(),
            volumeObjectId, info.getObjectID(), lastObjectId);

    dumpTableInfo(Types.FILE, effectivePath,
        metadataManager.getKeyTable(getBucketLayout()),
            volumeObjectId, info.getObjectID(), lastObjectId);
    metadataManager.stop();
  }

  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  private void dumpTableInfo(Types type,
      org.apache.hadoop.fs.Path effectivePath,
      Table<String, ? extends WithParentObjectId> table,
      long volumeId, long bucketId, long lastObjectId)
      throws IOException {
    MetadataKeyFilters.KeyPrefixFilter filter = getPrefixFilter(
            volumeId, bucketId, lastObjectId);

    List<? extends KeyValue
        <String, ? extends WithParentObjectId>> infoList =
        table.getRangeKVs(null, 1000, null, filter);

    for (KeyValue<String, ? extends WithParentObjectId> info :infoList) {
      Path key = Paths.get(info.getKey());
      dumpInfo(type, getEffectivePath(effectivePath,
          key.getName(1).toString()), info.getValue(), info.getKey());
    }
  }

  private org.apache.hadoop.fs.Path getEffectivePath(
      org.apache.hadoop.fs.Path currentPath, String name) {
    return new org.apache.hadoop.fs.Path(currentPath, name);
  }

  private void dumpInfo(Types level, org.apache.hadoop.fs.Path effectivePath,
                        WithParentObjectId id,  String key) {
    parserStats[level.ordinal()]++;
    System.out.println("Type:" + level);
    System.out.println("Path: " + effectivePath);
    System.out.println("DB Path: " + key);
    System.out.println("Object Id: " + id.getObjectID());
    System.out.println("Parent object Id: " + id.getParentObjectID());
    System.out.println();

  }

  private static MetadataKeyFilters.KeyPrefixFilter getPrefixFilter(
          long volumeId, long bucketId, long parentId) {
    String key = OM_KEY_PREFIX + volumeId +
            OM_KEY_PREFIX + bucketId +
            OM_KEY_PREFIX + parentId;
    return (new MetadataKeyFilters.KeyPrefixFilter())
        .addFilter(key);
  }

  public int getParserStats(Types type) {
    return parserStats[type.ordinal()];
  }
}

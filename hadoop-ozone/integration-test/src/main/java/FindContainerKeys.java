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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithParentObjectId;
import org.rocksdb.LiveFileMetaData;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES;

public class FindContainerKeys {
  private ManagedDBOptions dbOptions;
  private OzoneConfiguration conf = new OzoneConfiguration();
  private static final String DIRTREEDBNAME = "om.db";

  private RDBStore openDb(File omPath) {
    omPath.mkdirs();
    File dirTreeDbPath = new File(omPath, DIRTREEDBNAME);
    System.err.println("Creating database of dir tree path at " + dirTreeDbPath);
    try {
      // Delete the DB from the last run if it exists.
      //if (dirTreeDbPath.exists()) {
      //  FileUtils.deleteDirectory(dirTreeDbPath);
      //}
      // ConfigurationSource conf = new OzoneConfiguration();
      DBStoreBuilder dbStoreBuilder = DBStoreBuilder.newBuilder(conf);
      dbStoreBuilder.setName(dirTreeDbPath.getName());
      dbStoreBuilder.setPath(dirTreeDbPath.getParentFile().toPath());
      OmMetadataManagerImpl.addOMTablesAndCodecs(dbStoreBuilder);
      dbOptions = new ManagedDBOptions();
      dbOptions.setWalSizeLimitMB(100);
      dbOptions.setKeepLogFileNum(10);
      dbOptions.setMaxTotalWalSize(1000000000);
      dbOptions.setCreateIfMissing(true);
      dbOptions.setCreateMissingColumnFamilies(true);
      dbStoreBuilder.setDBOptions(dbOptions);
      return (RDBStore) dbStoreBuilder.build();
    } catch (IOException e) {
      System.err.println("Error creating omdirtree.db " + e);
      return null;
    }
  }

  private <T> void performOp(OmMetadataManagerImpl store, Table<String, T> table,
                             Function<List<Object[]>, Void> runnable)
      throws IOException, ExecutionException, InterruptedException {
    List<LiveFileMetaData> liveFileMetaDatas = ((RDBStore)store.getStore()).getDb().getLiveFilesMetaData();
    Set<String> sets = new HashSet<>();
    Queue<String> bounds = new PriorityQueue<>(String::compareTo);
    for (LiveFileMetaData liveFileMetaData : liveFileMetaDatas) {
      if (!StringUtils.bytes2String(liveFileMetaData.columnFamilyName()).equals(table.getName())) {
        continue;
      }
      String smallestKey = StringUtils.bytes2String(liveFileMetaData.smallestKey());
      String largestKey = StringUtils.bytes2String(liveFileMetaData.largestKey());
      if (!sets.contains(smallestKey)) {
        sets.add(smallestKey);
        bounds.add(smallestKey);
      }
      if (!sets.contains(largestKey)) {
        sets.add(largestKey);
        bounds.add(largestKey);
      }
    }
    List<String> vals = new ArrayList<>();
    while (!bounds.isEmpty()) {
      vals.add(bounds.poll());
    }
    System.out.println(vals.size());
    ThreadPoolExecutor pool = new ThreadPoolExecutor(30, 100, 1, TimeUnit.MINUTES, new ArrayBlockingQueue<>(2000));
    ThreadPoolExecutor workerPool = new ThreadPoolExecutor(30, 100, 1, TimeUnit.MINUTES,
        new ArrayBlockingQueue<>(40000));
    Queue<Future<?>> workers = new ConcurrentLinkedQueue<>();
    Queue<Future<?>> futures = new LinkedList<>();
    try {
      for (int i = 1; i < vals.size(); i++) {
        if (futures.size() == 100) {
          futures.poll().get();
        }
        System.out.println(i + " of " + (vals.size() - 1));
        String beg = vals.get(i - 1);
        String end = i == vals.size() -1 ? null : vals.get(i);
        futures.add(pool.submit(() -> {
          try (TableIterator<String, ? extends Table.KeyValue<String, T>> itr = table.iterator()) {
            System.out.println(table.getName() + "\t" + beg + "\t" + end);
            itr.seek(beg);
            while (itr.hasNext()) {
              List<Object[]> objs = new ArrayList<>();
              while (itr.hasNext() && objs.size() < 1000) {
                Table.KeyValue<String, T> val = itr.next();
                if (end != null && end.compareTo(val.getKey()) <=0) {
                  break;
                }
                objs.add(new Object[]{val.getKey(), val.getValue()});
              }
              if (!objs.isEmpty()) {
                if (workers.size() == 1000) {
                  Future<?> x = workers.poll();
                  if(x != null) {
                    x.get();
                  }
                }
                workers.add(workerPool.submit(() -> runnable.apply(objs)));
              } else {
                break;
              }
            }
          } catch (IOException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
          }
        }));
      }
      while (!futures.isEmpty()) {
        futures.poll().get();
      }
      while (!workers.isEmpty()) {
        Future<?> x = workers.poll();
        if (x != null) {
          x.get();
        }
      }

    } finally {
      pool.shutdown();
      workerPool.shutdown();
    }
  }

  public Function<List<Object[]>, Void> getParents(OmMetadataManagerImpl omMetadataManager, Map<Long, Object[]> maps,
   boolean deleted) {
    return (ks) -> {
      for (Object[] k : ks) {
        WithParentObjectId omDirInfo = (WithParentObjectId) k[1];
        String[] key = ((String)k[0]).split("/");
        long parentId = Long.parseLong(key[3]);
        String keyVal = key[4];
        maps.computeIfAbsent(omDirInfo.getObjectID(), (k1) -> new Object[]{keyVal, parentId, deleted});
      }
      return null;
    };
  }

  public Function<List<Object[]>, Void> getBuckets(OmMetadataManagerImpl omMetadataManager, Map<Long, Object[]> maps) {
    return (ks) -> {
      for (Object[] k : ks) {
        OmBucketInfo bucketInfo = (OmBucketInfo) k[1];
        maps.computeIfAbsent(bucketInfo.getObjectID(),
            (k1) -> new Object[]{ "/" + bucketInfo.getVolumeName() + "/" + bucketInfo.getBucketName(),
            null, false});
      }
      return null;
    };
  }

  private Pair<List<String>, String> getPath(Map<Long, Object[]> parents, Long parentId) {
    List<String> path = new ArrayList<>();
    boolean deleted = false;
    while (parentId != null) {
      if (parents.containsKey(parentId)) {
        path.add((String) parents.get(parentId)[0]);
        deleted = (boolean)parents.get(parentId)[2] || deleted;
        parentId = (Long) parents.get(parentId)[1];
      } else {
        System.out.println("Invalid parentId: " + parentId);
        break;
      }

    }
    Collections.reverse(path);
    return Pair.of(path, deleted ? "DELETED" : "PRESENT");
  }

  public Function<List<Object[]>, Void> getKeys(PrintWriter outFile, Set<Long> containerIds,
                                                Map<Long, Object[]> dirs, AtomicLong totalSize, AtomicLong cnt) {
    return  (ks) -> {
      for (Object[] k : ks) {
        List<ContainerBlockID> keyContainerIds = ((OmKeyInfo)k[1]).getKeyLocationVersions().stream()
            .flatMap(version -> version.getLocationList().stream().map(BlockLocationInfo::getBlockID)
                .map(BlockID::getContainerBlockID))
            .filter(blk -> containerIds.contains(blk.getContainerID()))
            .collect(Collectors.toList());
        if (!keyContainerIds.isEmpty()) {
          String[] key = ((String)k[0]).split("/");
          long parentId = Long.parseLong(key[3]);
          String keyVal = key[4];
          Pair<List<String>, String> path = getPath(dirs, parentId);

          outFile.println(k[0] + "\t" + path.getKey().stream().collect(Collectors.joining("/")) + "/" + keyVal
              + "\t" + path.getValue() + "\t" + ((OmKeyInfo)k[1]).getDataSize() + "\t" + keyContainerIds.stream()
              .map(blk -> blk.getContainerID() + ":" + blk.getLocalID())
              .collect(Collectors.joining(",")));
          totalSize.addAndGet(((OmKeyInfo)k[1]).getDataSize());
          if (cnt.incrementAndGet() % 10000 == 0) {
            System.out.println(cnt.toString() + "\t" + totalSize.toString());
          }
        }
      }
      return null;
    };
  }

//  private void flush(OmMetadataManagerImpl store) throws IOException {
//    store.getStore().flushLog(true);
//    store.getStore().flushDB();
//    try (ManagedCompactRangeOptions options = new ManagedCompactRangeOptions()) {
//      options.setBottommostLevelCompaction(CompactRangeOptions.BottommostLevelCompaction.kForce);
//      ((RDBStore)store.getStore()).compactDB(options);
//    }
//  }

  public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
    FindContainerKeys omdbReader = new FindContainerKeys();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.setInt(OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES, -1);
    OmMetadataManagerImpl omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration, new File(args[0]),
        "om.db");
//    omdbReader.flush(omMetadataManager);
    try (Stream<String> lines  = Files.lines(Paths.get(args[0] + "/containers.out"));
         PrintWriter outFile =
             new PrintWriter(new BufferedWriter(new FileWriter(args[1] +
                 omMetadataManager.getFileTable().getName() + "_missing_containers.txt")))) {
      AtomicLong totalSize = new AtomicLong(0);
      AtomicLong cnt = new AtomicLong(0);
      Set<Long> containerIds = lines.map(Long::valueOf).collect(Collectors.toSet());
      System.out.println("=================== DirectoryTable ======================");
      Map<Long, Object[]> dirs = new ConcurrentHashMap<>();
      Function<List<Object[]>, Void> bucketFunction = omdbReader.getBuckets(omMetadataManager, dirs);
      Function<List<Object[]>, Void> dirFunction = omdbReader.getParents(omMetadataManager, dirs, false);
      Function<List<Object[]>, Void> deletedDirFunction = omdbReader.getParents(omMetadataManager, dirs, true);
      Function<List<Object[]>, Void> function = omdbReader.getKeys(outFile, containerIds, dirs,
          totalSize, cnt);
      omdbReader.performOp(omMetadataManager, omMetadataManager.getBucketTable(), bucketFunction);
      omdbReader.performOp(omMetadataManager, omMetadataManager.getDeletedDirTable(), deletedDirFunction);
      omdbReader.performOp(omMetadataManager, omMetadataManager.getDirectoryTable(), dirFunction);
      omdbReader.performOp(omMetadataManager, omMetadataManager.getFileTable(), function);
    } finally {
      omMetadataManager.getStore().close();
    }
  }



}

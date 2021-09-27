/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.debug;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaOneImpl;

import org.kohsuke.MetaInfServices;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V1;

@CommandLine.Command(
    name = "cache-test",
    description = "Shell of updating datanode layout format",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
@MetaInfServices(SubcommandWithParent.class)
public class ContainerCacheTest extends GenericCli
    implements Callable<Void>, SubcommandWithParent{

  @CommandLine.Option(names = {"--path"},
      description = "File Path")
  private String storagePath;

  @CommandLine.Option(names = {"--numDB"},
      defaultValue = "1000",
      description = "")
  private int numDb;

  @CommandLine.Option(names = {"--cacheSize"},
      defaultValue = "1024",
      description = "")
  private int cacheSize;

  @CommandLine.Option(names = {"--stripes"},
      defaultValue = "1024",
      description = "")
  private int stripes;

  @CommandLine.Option(names = {"--numIterations"},
      defaultValue = "50000",
      description = "")
  private int iter;

  @CommandLine.Option(names = {"--timeoutInMins"},
      defaultValue = "20",
      description = "")
  private int timeout;

  @CommandLine.Option(names = {"--numThreads"},
      defaultValue = "100",
      description = "")
  private int numThreads;

  @CommandLine.Option(names = {"--skipDBCreation"},
      defaultValue = "false",
      description = "Skip creation of DBs at start, useful to rerun the same test")
  private boolean skipDBCreation;

  @CommandLine.Option(names = {"--numKeysPerIter"},
      defaultValue = "10",
      description = "")
  private int numKeysPerIter;

  @Spec
  private CommandSpec spec;

  @Override
  public Void call() throws Exception {
    OzoneConfiguration conf = createOzoneConfiguration();
    run(conf, storagePath, numDb, cacheSize, stripes, iter, timeout,
        numThreads, numKeysPerIter, skipDBCreation);
    return null;
  }

  public static void main(String[] args) {
    new DatanodeLayout().run(args);
  }

  @Override
  public Class<?> getParentType() {
    return OzoneDebug.class;
  }

  public static void run(OzoneConfiguration conf,
                                            String storagePaths,
                                            int numDb,
                                            int cacheSize,
                                            int stripes,
                                            int iter, int timeout,
                         int numThreads, int numKeysPerIter, boolean skipDBCreation) throws Exception {

    conf.setInt(OzoneConfigKeys.OZONE_CONTAINER_CACHE_SIZE, cacheSize);
    conf.setInt(OzoneConfigKeys.OZONE_CONTAINER_CACHE_LOCK_STRIPES, stripes);
    ContainerCache cache = ContainerCache.getInstance(conf);
    cache.clear();

    String[] paths = storagePaths.split(",");
    int numPaths = paths.length;
    for (int i = 0; i < numPaths; i ++) {
      File root = new File(paths[i]);
      root.mkdirs();
    }

    if (!skipDBCreation) {
      for (int i = 0; i < numDb; i++) {
        File root = new File(paths[i % numPaths]);
        File containerDir1 = new File(root, "cont" + i);
        createContainerDB(conf, containerDir1);
        System.out.println("Created DB " + (i + 1));
      }
    }

    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

    AtomicInteger count = new AtomicInteger(0);

    for (int i = 0; i < iter; i++) {
      int finalI1 = i;
      executorService.execute(() -> {
        int cont = new Random().nextInt(numDb);
        File root = new File(paths[cont % numPaths]);
        File containerDir1 = new File(root, "cont" + cont);
        ReferenceCountedDB refcountedDB = null;
        try {
          refcountedDB = cache.getDB(1, "RocksDB",
            containerDir1.getPath(), SCHEMA_V1, conf);
          DatanodeStore store = refcountedDB.getStore();
/*          for (int j = 0; j < numKeysPerIter; j++) {
            store.getMetadataTable().put(String.valueOf(System.currentTimeMillis()), (long) j);
          }
          store.flushLog(true);
          for (int j = 0; j < 10; j++) {
            store.getMetadataTable().put(String.valueOf(System.currentTimeMillis()), (long) j);
          }
          store.flushLog(true);*/
          System.out.println("Count = " + count.incrementAndGet());
        } catch (IOException e) {
          e.printStackTrace();
        } finally {
          if (refcountedDB != null) {
            refcountedDB.close();
          }
        }
      });
    }
    executorService.shutdown();
    executorService.awaitTermination(timeout, TimeUnit.MINUTES);
  }

  private static void createContainerDB(OzoneConfiguration conf, File dbFile)
      throws Exception {
    DatanodeStore store = new DatanodeStoreSchemaOneImpl(
        conf, 1, dbFile.getAbsolutePath(), false);

    // we close since the SCM pre-creates containers.
    // we will open and put Db handle into a cache when keys are being created
    // in a container.

    store.stop();
  }
}
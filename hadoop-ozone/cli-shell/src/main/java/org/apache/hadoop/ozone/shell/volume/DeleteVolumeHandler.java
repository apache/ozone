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

package org.apache.hadoop.ozone.shell.volume;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.hdds.scm.net.NetConstants.PATH_SEPARATOR_STR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Executes deleteVolume call for the shell.
 */
@Command(name = "delete",
    description = "deletes a volume")
public class DeleteVolumeHandler extends VolumeHandler {
  @CommandLine.Option(
      names = {"-r"},
      description = "Delete volume recursively"
  )
  private boolean bRecursive;
  @CommandLine.Option(names = {"-t", "--threads", "--thread"},
      description = "Number of threads used to execute recursive delete")
  private int threadNo = 10;
  @CommandLine.Option(names = {"-y", "--yes"},
      description = "Continue without interactive user confirmation")
  private boolean yes;
  private List<String> bucketIdList = new ArrayList<>();
  private AtomicInteger cleanedBucketCounter =
      new AtomicInteger();
  private int totalBucketCount;
  private OzoneVolume vol;
  private AtomicInteger numberOfBucketsCleaned = new AtomicInteger(0);
  private volatile Throwable exception;
  private static final int MAX_KEY_DELETE_BATCH_SIZE = 1000;
  private String omServiceId;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    String volumeName = address.getVolumeName();
    omServiceId = address.getOmServiceId(getConf());
    try {
      if (bRecursive) {
        if (!yes) {
          // Ask for user confirmation
          out().print("This command will delete volume recursively." +
              "\nThere is no recovery option after using this command, " +
              "and no trash for FSO buckets." +
              "\nDelay is expected running this command." +
              "\nEnter 'yes' to proceed': ");
          out().flush();
          Scanner scanner = new Scanner(new InputStreamReader(
              System.in, StandardCharsets.UTF_8));
          String confirmation = scanner.next().trim().toLowerCase();
          if (!confirmation.equals("yes")) {
            out().println("Operation cancelled.");
            return;
          }
        }
        vol = client.getObjectStore().getVolume(volumeName);
        deleteVolumeRecursive();
      }
    } catch (InterruptedException e) {
      out().printf("Exception while deleting volume recursively%n");
      return;
    }
    client.getObjectStore().deleteVolume(volumeName);
    out().printf("Volume %s is deleted%n", volumeName);
  }

  private void deleteVolumeRecursive()
      throws InterruptedException {
    // Get all the buckets for given volume
    Iterator<? extends OzoneBucket> bucketIterator =
        vol.listBuckets(null);

    while (bucketIterator.hasNext()) {
      OzoneBucket bucket = bucketIterator.next();
      bucketIdList.add(bucket.getName());
      totalBucketCount++;
    }
    doCleanBuckets();
    // Reset counters and bucket list
    numberOfBucketsCleaned.set(0);
    totalBucketCount = 0;
    cleanedBucketCounter.set(0);
    bucketIdList.clear();
  }

  /**
   * Clean OBS bucket recursively.
   *
   * @param  bucket OzoneBucket
   * @return boolean
   */
  private boolean cleanOBSBucket(OzoneBucket bucket) {
    ArrayList<String> keys = new ArrayList<>();
    try {
      if (!bucket.isLink()) {
        Iterator<? extends OzoneKey> iterator = bucket.listKeys(null);
        while (iterator.hasNext()) {
          keys.add(iterator.next().getName());
          if (MAX_KEY_DELETE_BATCH_SIZE == keys.size()) {
            bucket.deleteKeys(keys);
            keys.clear();
          }
        }
        // delete if any remaining keys left
        if (!keys.isEmpty()) {
          bucket.deleteKeys(keys);
        }
      }
      vol.deleteBucket(bucket.getName());
      numberOfBucketsCleaned.getAndIncrement();
      return true;
    } catch (Exception e) {
      LOG.error("Could not clean bucket ", e);
      return false;
    }
  }

  /**
   * Clean Legacy/FSO bucket recursively.
   *
   * @param  bucket OzoneBucket
   * @return boolean
   */
  private boolean cleanFSBucket(OzoneBucket bucket) {
    try {
      String hostPrefix = OZONE_OFS_URI_SCHEME + "://" +
          omServiceId + PATH_SEPARATOR_STR;
      String ofsPrefix = hostPrefix + vol.getName() + PATH_SEPARATOR_STR +
          bucket.getName();
      final Path path = new Path(ofsPrefix);
      OzoneConfiguration clientConf = new OzoneConfiguration(getConf());
      clientConf.set(FS_DEFAULT_NAME_KEY, hostPrefix);
      try (FileSystem fs = FileSystem.get(clientConf)) {
        if (!fs.delete(path, true)) {
          throw new IOException("Failed to delete bucket");
        }
      }
      numberOfBucketsCleaned.getAndIncrement();
      return true;
    } catch (Exception e) {
      exception = e;
      LOG.error("Could not clean bucket ", e);
      return false;
    }
  }

  private class BucketCleaner implements Runnable {
    @Override
    public void run() {
      int i;
      while ((i = cleanedBucketCounter.getAndIncrement()) < totalBucketCount) {
        try {
          OzoneBucket bucket = vol.getBucket(bucketIdList.get(i));
          switch (bucket.getBucketLayout()) {
          case FILE_SYSTEM_OPTIMIZED:
          case LEGACY:
            if (!cleanFSBucket(bucket)) {
              throw new RuntimeException("Failed to clean bucket");
            }
            break;
          case OBJECT_STORE:
            if (!cleanOBSBucket(bucket)) {
              throw new RuntimeException("Failed to clean bucket");
            }
            break;
          default:
            throw new RuntimeException("Invalid bucket layout");
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private void doCleanBuckets() throws InterruptedException {
    ExecutorService executor = Executors.newFixedThreadPool(threadNo);
    for (int i = 0; i < threadNo; i++) {
      executor.execute(new BucketCleaner());
    }

    try {
      // wait until all Buckets are cleaned or exception occurred.
      while (numberOfBucketsCleaned.get() != totalBucketCount
          && exception == null) {
        Thread.sleep(100);
      }
    } catch (InterruptedException e) {
      LOG.error("Failed to wait until all Buckets are cleaned", e);
      Thread.currentThread().interrupt();
    }
    executor.shutdown();
    executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
  }
}

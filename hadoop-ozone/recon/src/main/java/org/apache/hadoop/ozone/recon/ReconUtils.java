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

package org.apache.hadoop.ozone.recon;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.inject.Singleton;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmUtils;
import org.apache.hadoop.hdds.scm.ha.SCMNodeDetails;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.io.IOUtils;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_EVENT_CONTAINER_REPORT_QUEUE_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_EVENT_THREAD_POOL_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.server.ServerUtils.getDirectoryFromConfig;
import static org.apache.hadoop.hdds.server.ServerUtils.getOzoneMetaDirPath;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_DB_DIR;
import static org.jooq.impl.DSL.currentTimestamp;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.using;

import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconContainerReportQueue;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.hadoop.ozone.recon.schema.tables.pojos.GlobalStats;
import org.jetbrains.annotations.NotNull;
import com.google.common.annotations.VisibleForTesting;
import org.jooq.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon Utility class.
 */
@Singleton
public class ReconUtils {

  private static final int WRITE_BUFFER = 1048576; //1MB

  public ReconUtils() {
  }

  private static Logger log = LoggerFactory.getLogger(
      ReconUtils.class);

  private static AtomicBoolean rebuildTriggered = new AtomicBoolean(false);

  public static File getReconScmDbDir(ConfigurationSource conf) {
    return new ReconUtils().getReconDbDir(conf, OZONE_RECON_SCM_DB_DIR);
  }

  @NotNull
  public static List<BlockingQueue<SCMDatanodeHeartbeatDispatcher
      .ContainerReport>> initContainerReportQueue(
      OzoneConfiguration configuration) {
    int threadPoolSize =
        configuration.getInt(ScmUtils.getContainerReportConfPrefix()
                + ".thread.pool.size",
            OZONE_SCM_EVENT_THREAD_POOL_SIZE_DEFAULT);
    int queueSize = configuration.getInt(
        ScmUtils.getContainerReportConfPrefix() + ".queue.size",
        OZONE_SCM_EVENT_CONTAINER_REPORT_QUEUE_SIZE_DEFAULT);
    List<BlockingQueue<SCMDatanodeHeartbeatDispatcher.ContainerReport>> queues =
        new ArrayList<>();
    for (int i = 0; i < threadPoolSize; ++i) {
      queues.add(new ReconContainerReportQueue(queueSize));
    }
    return queues;
  }

  /**
   * Get configured Recon DB directory value based on config. If not present,
   * fallback to ozone.metadata.dirs
   *
   * @param conf         configuration bag
   * @param dirConfigKey key to check
   * @return Return File based on configured or fallback value.
   */
  public File getReconDbDir(ConfigurationSource conf, String dirConfigKey) {

    File metadataDir = getDirectoryFromConfig(conf, dirConfigKey,
        "Recon");
    if (metadataDir != null) {
      return metadataDir;
    }

    log.warn("{} is not configured. We recommend adding this setting. " +
            "Falling back to {} instead.",
        dirConfigKey, HddsConfigKeys.OZONE_METADATA_DIRS);
    return getOzoneMetaDirPath(conf);
  }

  /**
   * Given a source directory, create a tar file from it.
   *
   * @param sourcePath the path to the directory to be archived.
   * @return tar file
   * @throws IOException
   */
  public static File createTarFile(Path sourcePath) throws IOException {
    TarArchiveOutputStream tarOs = null;
    FileOutputStream fileOutputStream = null;
    try {
      String sourceDir = sourcePath.toString();
      String fileName = sourceDir.concat(".tar");
      fileOutputStream = new FileOutputStream(fileName);
      tarOs = new TarArchiveOutputStream(fileOutputStream);
      tarOs.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
      File folder = new File(sourceDir);
      File[] filesInDir = folder.listFiles();
      if (filesInDir != null) {
        for (File file : filesInDir) {
          addFilesToArchive(file.getName(), file, tarOs);
        }
      }
      return new File(fileName);
    } finally {
      try {
        org.apache.hadoop.io.IOUtils.closeStream(tarOs);
        org.apache.hadoop.io.IOUtils.closeStream(fileOutputStream);
      } catch (Exception e) {
        log.error("Exception encountered when closing " +
            "TAR file output stream: " + e);
      }
    }
  }

  private static void addFilesToArchive(String source, File file,
                                        TarArchiveOutputStream
                                            tarFileOutputStream)
      throws IOException {
    tarFileOutputStream.putArchiveEntry(new TarArchiveEntry(file, source));
    if (file.isFile()) {
      try (FileInputStream fileInputStream = new FileInputStream(file)) {
        BufferedInputStream bufferedInputStream =
            new BufferedInputStream(fileInputStream);
        org.apache.commons.compress.utils.IOUtils.copy(bufferedInputStream,
            tarFileOutputStream);
        tarFileOutputStream.closeArchiveEntry();
      }
    } else if (file.isDirectory()) {
      tarFileOutputStream.closeArchiveEntry();
      File[] filesInDir = file.listFiles();
      if (filesInDir != null) {
        for (File cFile : filesInDir) {
          addFilesToArchive(cFile.getAbsolutePath(), cFile,
              tarFileOutputStream);
        }
      }
    }
  }

  /**
   * Untar DB snapshot tar file to recon OM snapshot directory.
   *
   * @param tarFile  source tar file
   * @param destPath destination path to untar to.
   * @throws IOException ioException
   */
  public void untarCheckpointFile(File tarFile, Path destPath)
      throws IOException {

    FileInputStream fileInputStream = null;
    try {
      fileInputStream = new FileInputStream(tarFile);

      //Create Destination directory if it does not exist.
      if (!destPath.toFile().exists()) {
        boolean success = destPath.toFile().mkdirs();
        if (!success) {
          throw new IOException("Unable to create Destination directory.");
        }
      }

      try (TarArchiveInputStream tarInStream =
               new TarArchiveInputStream(fileInputStream)) {
        TarArchiveEntry entry;

        while ((entry = (TarArchiveEntry) tarInStream.getNextEntry()) != null) {
          Path path = Paths.get(destPath.toString(), entry.getName());
          HddsUtils.validatePath(path, destPath);
          File f = path.toFile();
          //If directory, create a directory.
          if (entry.isDirectory()) {
            boolean success = f.mkdirs();
            if (!success) {
              log.error("Unable to create directory found in tar.");
            }
          } else {
            //Write contents of file in archive to a new file.
            int count;
            byte[] data = new byte[WRITE_BUFFER];

            FileOutputStream fos = new FileOutputStream(f);
            try (BufferedOutputStream dest =
                     new BufferedOutputStream(fos, WRITE_BUFFER)) {
              while ((count =
                  tarInStream.read(data, 0, WRITE_BUFFER)) != -1) {
                dest.write(data, 0, count);
              }
            }
          }
        }
      }
    } finally {
      IOUtils.closeStream(fileInputStream);
    }
  }


  /**
   * Constructs the full path of a key from its OmKeyInfo using a bottom-up approach, starting from the leaf node.
   *
   * The method begins with the leaf node (the key itself) and recursively prepends parent directory names, fetched
   * via NSSummary objects, until reaching the parent bucket (parentId is -1). It effectively builds the path from
   * bottom to top, finally prepending the volume and bucket names to complete the full path. If the directory structure
   * is currently being rebuilt (indicated by the rebuildTriggered flag), this method returns an empty string to signify
   * that path construction is temporarily unavailable.
   *
   * @param omKeyInfo The OmKeyInfo object for the key
   * @return The constructed full path of the key as a String, or an empty string if a rebuild is in progress and
   *         the path cannot be constructed at this time.
   * @throws IOException
   */
  public static String constructFullPath(OmKeyInfo omKeyInfo,
                                         ReconNamespaceSummaryManager reconNamespaceSummaryManager,
                                         ReconOMMetadataManager omMetadataManager)
      throws IOException {

    StringBuilder fullPath = new StringBuilder(omKeyInfo.getKeyName());
    long parentId = omKeyInfo.getParentObjectID();
    boolean isDirectoryPresent = false;

    while (parentId != 0) {
      NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(parentId);
      if (nsSummary == null) {
        log.warn("NSSummary tree is currently being rebuilt or the directory could be in the progress of " +
            "deletion, returning empty string for path construction.");
        return "";
      }
      if (nsSummary.getParentId() == -1) {
        if (rebuildTriggered.compareAndSet(false, true)) {
          triggerRebuild(reconNamespaceSummaryManager, omMetadataManager);
        }
        log.warn("NSSummary tree is currently being rebuilt, returning empty string for path construction.");
        return "";
      }
      fullPath.insert(0, nsSummary.getDirName() + OM_KEY_PREFIX);

      // Move to the parent ID of the current directory
      parentId = nsSummary.getParentId();
      isDirectoryPresent = true;
    }

    // Prepend the volume and bucket to the constructed path
    String volumeName = omKeyInfo.getVolumeName();
    String bucketName = omKeyInfo.getBucketName();
    fullPath.insert(0, volumeName + OM_KEY_PREFIX + bucketName + OM_KEY_PREFIX);
    if (isDirectoryPresent) {
      return OmUtils.normalizeKey(fullPath.toString(), true);
    }
    return fullPath.toString();
  }

  private static void triggerRebuild(ReconNamespaceSummaryManager reconNamespaceSummaryManager,
                                     ReconOMMetadataManager omMetadataManager) {
    ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r);
      t.setName("RebuildNSSummaryThread");
      return t;
    });

    executor.submit(() -> {
      long startTime = System.currentTimeMillis();
      log.info("Rebuilding NSSummary tree...");
      try {
        reconNamespaceSummaryManager.rebuildNSSummaryTree(omMetadataManager);
      } finally {
        long endTime = System.currentTimeMillis();
        log.info("NSSummary tree rebuild completed in {} ms.", endTime - startTime);
      }
    });
    executor.shutdown();
  }

  /**
   * Make HTTP GET call on the URL and return HttpURLConnection instance.
   *
   * @param connectionFactory URLConnectionFactory to use.
   * @param url               url to call
   * @param isSpnego          is SPNEGO enabled
   * @return HttpURLConnection instance of the HTTP call.
   * @throws IOException, AuthenticationException While reading the response.
   */
  public HttpURLConnection makeHttpCall(URLConnectionFactory connectionFactory,
                                        String url, boolean isSpnego)
      throws IOException, AuthenticationException {
    HttpURLConnection urlConnection = (HttpURLConnection)
        connectionFactory.openConnection(new URL(url), isSpnego);
    urlConnection.connect();
    return urlConnection;
  }

  /**
   * Load last known DB in Recon.
   *
   * @param reconDbDir
   * @param fileNamePrefix
   * @return
   */
  public File getLastKnownDB(File reconDbDir, String fileNamePrefix) {
    String lastKnownSnapshotFileName = null;
    long lastKnonwnSnapshotTs = Long.MIN_VALUE;
    if (reconDbDir != null) {
      File[] snapshotFiles = reconDbDir.listFiles((dir, name) ->
          name.startsWith(fileNamePrefix));
      if (snapshotFiles != null) {
        for (File snapshotFile : snapshotFiles) {
          String fileName = snapshotFile.getName();
          try {
            String[] fileNameSplits = fileName.split("_");
            if (fileNameSplits.length <= 1) {
              continue;
            }
            long snapshotTimestamp = Long.parseLong(fileNameSplits[1]);
            if (lastKnonwnSnapshotTs < snapshotTimestamp) {
              lastKnonwnSnapshotTs = snapshotTimestamp;
              lastKnownSnapshotFileName = fileName;
            }
          } catch (NumberFormatException nfEx) {
            log.warn("Unknown file found in Recon DB dir : {}", fileName);
          }
        }
      }
    }
    return lastKnownSnapshotFileName == null ? null :
        new File(reconDbDir.getPath(), lastKnownSnapshotFileName);
  }

  /**
   * Upsert row in GlobalStats table.
   *
   * @param sqlConfiguration
   * @param globalStatsDao
   * @param key
   * @param count
   */
  public static void upsertGlobalStatsTable(Configuration sqlConfiguration,
                                            GlobalStatsDao globalStatsDao,
                                            String key,
                                            Long count) {
    // Get the current timestamp
    Timestamp now =
        using(sqlConfiguration).fetchValue(select(currentTimestamp()));
    GlobalStats record = globalStatsDao.fetchOneByKey(key);
    GlobalStats newRecord = new GlobalStats(key, count, now);

    // Insert a new record for key if it does not exist
    if (record == null) {
      globalStatsDao.insert(newRecord);
    } else {
      globalStatsDao.update(newRecord);
    }
  }

  /**
   * Sorts a list of DiskUsage objects in descending order by size using parallel sorting and
   * returns the top N records as specified by the limit.
   *
   * This method is optimized for large datasets and utilizes parallel processing to efficiently
   * sort and retrieve the top N largest records by size. It's especially useful for reducing
   * processing time and memory usage when only a subset of sorted records is needed.
   *
   * Advantages of this approach include:
   * - Efficient handling of large datasets by leveraging multi-core processors.
   * - Reduction in memory usage and improvement in processing time by limiting the
   *   number of returned records.
   * - Scalability and easy integration with existing systems.
   *
   * @param diskUsageList the list of DiskUsage objects to be sorted.
   * @param limit the maximum number of DiskUsage objects to return.
   * @return a list of the top N DiskUsage objects sorted in descending order by size,
   *  where N is the specified limit.
   */
  public static List<DUResponse.DiskUsage> sortDiskUsageDescendingWithLimit(
      List<DUResponse.DiskUsage> diskUsageList, int limit) {
    return diskUsageList.parallelStream()
        .sorted((du1, du2) -> Long.compare(du2.getSize(), du1.getSize()))
        .limit(limit)
        .collect(Collectors.toList());
  }

  public static long getFileSizeUpperBound(long fileSize) {
    if (fileSize >= ReconConstants.MAX_FILE_SIZE_UPPER_BOUND) {
      return Long.MAX_VALUE;
    }
    // The smallest file size being tracked for count
    // is 1 KB i.e. 1024 = 2 ^ 10.
    int binIndex = getFileSizeBinIndex(fileSize);
    return (long) Math.pow(2, (10 + binIndex));
  }

  public static long getContainerSizeUpperBound(long containerSize) {
    if (containerSize >= ReconConstants.MAX_CONTAINER_SIZE_UPPER_BOUND) {
      return Long.MAX_VALUE;
    }
    // The smallest container size being tracked for count
    // is 512MB i.e. 536870912L = 2 ^ 29.
    int binIndex = getContainerSizeBinIndex(containerSize);
    return (long) Math.pow(2, (29 + binIndex));
  }


  public static int getFileSizeBinIndex(long fileSize) {
    Preconditions.checkArgument(fileSize >= 0,
        "fileSize = %s < 0", fileSize);
    // if the file size is larger than our track scope,
    // we map it to the last bin
    if (fileSize >= ReconConstants.MAX_FILE_SIZE_UPPER_BOUND) {
      return ReconConstants.NUM_OF_FILE_SIZE_BINS - 1;
    }
    int index = nextClosestPowerIndexOfTwo(fileSize);
    // if the file size is smaller than our track scope,
    // we map it to the first bin
    return index < 10 ? 0 : index - 10;
  }

  public static int getContainerSizeBinIndex(long containerSize) {
    Preconditions.checkArgument(containerSize >= 0,
        "containerSize = %s < 0", containerSize);
    // if the container size is larger than our track scope,
    // we map it to the last bin
    if (containerSize >= ReconConstants.MAX_CONTAINER_SIZE_UPPER_BOUND) {
      return ReconConstants.NUM_OF_CONTAINER_SIZE_BINS - 1;
    }
    int index = nextClosestPowerIndexOfTwo(containerSize);
    // if the container size is smaller than our track scope,
    // we map it to the first bin
    return index < 29 ? 0 : index - 29;
  }

  static int nextClosestPowerIndexOfTwo(long n) {
    return n > 0 ? 64 - Long.numberOfLeadingZeros(n - 1)
        : n == 0 ? 0
        : n == Long.MIN_VALUE ? -63
        : -nextClosestPowerIndexOfTwo(-n);
  }

  public SCMNodeDetails getReconNodeDetails(OzoneConfiguration conf) {
    SCMNodeDetails.Builder builder = new SCMNodeDetails.Builder();
    builder.setSCMNodeId("Recon");
    builder.setDatanodeProtocolServerAddress(
        HddsServerUtil.getReconDataNodeBindAddress(conf));
    return builder.build();
  }

  @VisibleForTesting
  public static void setLogger(Logger logger) {
    log = logger;
  }
}

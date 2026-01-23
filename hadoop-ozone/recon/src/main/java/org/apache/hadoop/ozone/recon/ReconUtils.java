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

package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_EVENT_CONTAINER_REPORT_QUEUE_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_EVENT_THREAD_POOL_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.server.ServerUtils.getDirectoryFromConfig;
import static org.apache.hadoop.hdds.server.ServerUtils.getOzoneMetaDirPath;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_DB_DIR;
import static org.jooq.impl.DSL.currentTimestamp;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.using;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Singleton;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmUtils;
import org.apache.hadoop.hdds.scm.ha.SCMNodeDetails;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher;
import org.apache.hadoop.hdds.utils.Archiver;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.api.handlers.BucketHandler;
import org.apache.hadoop.ozone.recon.api.handlers.EntityHandler;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconContainerReportQueue;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.recon.schema.generated.tables.daos.GlobalStatsDao;
import org.apache.ozone.recon.schema.generated.tables.pojos.GlobalStats;
import org.jooq.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon Utility class.
 */
@Singleton
public class ReconUtils {

  private static Logger log = LoggerFactory.getLogger(
      ReconUtils.class);

  public ReconUtils() {
  }

  /**
   * Get the current rebuild state of NSSummary tree.
   * Delegates to NSSummaryTask's unified control mechanism.
   *
   * @return current RebuildState from NSSummaryTask
   */
  public static org.apache.hadoop.ozone.recon.tasks.NSSummaryTask.RebuildState getNSSummaryRebuildState() {
    return org.apache.hadoop.ozone.recon.tasks.NSSummaryTask.getRebuildState();
  }

  public static File getReconScmDbDir(ConfigurationSource conf) {
    return new ReconUtils().getReconDbDir(conf, OZONE_RECON_SCM_DB_DIR);
  }

  @Nonnull
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
   */
  public static File createTarFile(Path sourcePath) throws IOException {
    String source = StringUtils.removeEnd(sourcePath.toString(), "/");
    File tarFile = new File(source.concat(".tar"));
    Archiver.create(tarFile, sourcePath);
    return tarFile;
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
    Archiver.extract(tarFile, destPath);
  }

  /**
   * Constructs the full path of a key from its OmKeyInfo using a bottom-up approach, starting from the leaf node.
   * <p>
   * The method begins with the leaf node (the key itself) and recursively prepends parent directory names, fetched
   * via NSSummary objects, until reaching the parent bucket (parentId is -1). It effectively builds the path from
   * bottom to top, finally prepending the volume and bucket names to complete the full path. If the directory structure
   * is currently being rebuilt (indicated by the rebuildTriggered flag), this method returns an empty string to signify
   * that path construction is temporarily unavailable.
   *
   * @param omKeyInfo The OmKeyInfo object for the key
   * @return The constructed full path of the key as a String, or an empty string if a rebuild is in progress and
   * the path cannot be constructed at this time.
   * @throws IOException
   */
  public static String constructFullPath(OmKeyInfo omKeyInfo,
                                         ReconNamespaceSummaryManager reconNamespaceSummaryManager) throws IOException {
    return constructFullPath(omKeyInfo.getKeyName(), omKeyInfo.getParentObjectID(), omKeyInfo.getVolumeName(),
        omKeyInfo.getBucketName(), reconNamespaceSummaryManager);
  }

  /**
   * Constructs the full path of a key from its key name and parent ID using a bottom-up approach, starting from the
   * leaf node.
   * <p>
   * The method begins with the leaf node (the key itself) and recursively prepends parent directory names, fetched
   * via NSSummary objects, until reaching the parent bucket (parentId is -1). It effectively builds the path from
   * bottom to top, finally prepending the volume and bucket names to complete the full path. If the directory structure
   * is currently being rebuilt (indicated by the rebuildTriggered flag), this method returns an empty string to signify
   * that path construction is temporarily unavailable.
   *
   * @param keyName         The name of the key
   * @param initialParentId The parent ID of the key
   * @param volumeName      The name of the volume
   * @param bucketName      The name of the bucket
   * @return The constructed full path of the key as a String, or an empty string if a rebuild is in progress and
   * the path cannot be constructed at this time.
   * @throws IOException
   */
  public static String constructFullPath(String keyName, long initialParentId, String volumeName, String bucketName,
                                         ReconNamespaceSummaryManager reconNamespaceSummaryManager) throws IOException {
    StringBuilder fullPath = constructFullPathPrefix(initialParentId, volumeName, bucketName,
        reconNamespaceSummaryManager);
    if (fullPath.length() == 0) {
      return "";
    }
    fullPath.append(keyName);
    return fullPath.toString();
  }

  /**
   * Constructs the prefix path to a key from its key name and parent ID using a bottom-up approach, starting from the
   * leaf node.
   * <p>
   * The method begins with the leaf node (the key itself) and recursively prepends parent directory names, fetched
   * via NSSummary objects, until reaching the parent bucket (parentId is -1). It effectively builds the path from
   * bottom to top, finally prepending the volume and bucket names to complete the full path. If the directory structure
   * is currently being rebuilt (indicated by the rebuildTriggered flag), this method returns an empty string to signify
   * that path construction is temporarily unavailable.
   *
   * @param initialParentId The parent ID of the key
   * @param volumeName      The name of the volume
   * @param bucketName      The name of the bucket
   * @return A StringBuilder containing the constructed prefix path of the key, or an empty string builder if a rebuild
   * is in progress.
   * @throws IOException
   */
  public static StringBuilder constructFullPathPrefix(long initialParentId, String volumeName,
      String bucketName, ReconNamespaceSummaryManager reconNamespaceSummaryManager) throws IOException {

    StringBuilder fullPath = new StringBuilder();
    long parentId = initialParentId;
    boolean isDirectoryPresent = false;

    List<String> pathSegments = new ArrayList<>();
    while (parentId != 0) {
      NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(parentId);
      if (nsSummary == null) {
        log.warn("NSSummary tree is currently being rebuilt or the directory could be in the progress of " +
            "deletion, returning empty string for path construction.");
        fullPath.setLength(0);
        return fullPath;
      }
      // On the last pass, dir-name will be empty and parent will be zero, indicating the loop should end.
      if (!nsSummary.getDirName().isEmpty()) {
        pathSegments.add(nsSummary.getDirName());
      }

      // Move to the parent ID of the current directory
      parentId = nsSummary.getParentId();
      isDirectoryPresent = true;
    }

    fullPath.append(volumeName).append(OM_KEY_PREFIX)
        .append(bucketName).append(OM_KEY_PREFIX);

    // Build the components in a list, then reverse and join once
    for (int i = pathSegments.size() - 1; i >= 0; i--) {
      fullPath.append(pathSegments.get(i)).append(OM_KEY_PREFIX);
    }

    // TODO - why is this needed? It seems lke it should handle double slashes in the path name,
    //        but its not clear how they get there. This normalize call is quite expensive as it
    //        creates several objects (URI, PATH, back to string). There was a bug fixed above
    //        where the last parent dirName was empty, which always caused a double // after the
    //        bucket name, but with that fixed, it seems like this should not be needed. All tests
    //        pass without it for key listing.
    if (isDirectoryPresent) {
      if (fullPath.indexOf("//") >= 0) {
        String path = fullPath.toString();
        fullPath.setLength(0);
        fullPath.append(OmUtils.normalizeKey(path, true));
      }
    }
    return fullPath;
  }

  /**
   * Converts a key prefix into an object path for FSO buckets, using IDs.
   *
   * This method transforms a user-provided path (e.g., "volume/bucket/dir1") into
   * a database-friendly format ("/volumeID/bucketID/ParentId/") by replacing names
   * with their corresponding IDs. It simplifies database queries for FSO bucket operations.
   * <pre>
   * {@code
   * Examples:
   * - Input: "volume/bucket/key" -> Output: "/volumeID/bucketID/parentDirID/key"
   * - Input: "volume/bucket/dir1" -> Output: "/volumeID/bucketID/dir1ID/"
   * - Input: "volume/bucket/dir1/key1" -> Output: "/volumeID/bucketID/dir1ID/key1"
   * - Input: "volume/bucket/dir1/dir2" -> Output: "/volumeID/bucketID/dir2ID/"
   * }
   * </pre>
   * @param prevKeyPrefix The path to be converted.
   * @return The object path as "/volumeID/bucketID/ParentId/" or an empty string if an error occurs.
   * @throws IOException If database access fails.
   * @throws IllegalArgumentException If the provided path is invalid or cannot be converted.
   */
  public static String convertToObjectPathForOpenKeySearch(String prevKeyPrefix,
                                                           ReconOMMetadataManager omMetadataManager,
                                                           ReconNamespaceSummaryManager reconNamespaceSummaryManager,
                                                           OzoneStorageContainerManager reconSCM)
      throws IOException {
    try {
      String[] names = EntityHandler.parseRequestPath(EntityHandler.normalizePath(
          prevKeyPrefix, BucketLayout.FILE_SYSTEM_OPTIMIZED));
      Table<String, OmKeyInfo> openFileTable = omMetadataManager.getOpenKeyTable(
          BucketLayout.FILE_SYSTEM_OPTIMIZED);

      // Root-Level: Return the original path
      if (names.length == 0 || names[0].isEmpty()) {
        return prevKeyPrefix;
      }

      // Volume-Level: Fetch the volumeID
      String volumeName = names[0];
      validateNames(volumeName);
      String volumeKey = omMetadataManager.getVolumeKey(volumeName);
      long volumeId = omMetadataManager.getVolumeTable().getSkipCache(volumeKey).getObjectID();
      if (names.length == 1) {
        return constructObjectPathWithPrefix(volumeId);
      }

      // Bucket-Level: Fetch the bucketID
      String bucketName = names[1];
      validateNames(bucketName);
      String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
      OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().getSkipCache(bucketKey);
      long bucketId = bucketInfo.getObjectID();
      if (names.length == 2 || bucketInfo.getBucketLayout() != BucketLayout.FILE_SYSTEM_OPTIMIZED) {
        return constructObjectPathWithPrefix(volumeId, bucketId);
      }

      // Directory or Key-Level: Check both key and directory
      BucketHandler handler =
          BucketHandler.getBucketHandler(reconNamespaceSummaryManager, omMetadataManager, reconSCM, bucketInfo);

      if (names.length >= 3) {
        String lastEntiry = names[names.length - 1];

        // Check if the directory exists
        OmDirectoryInfo dirInfo = handler.getDirInfo(names);
        if (dirInfo != null && dirInfo.getName().equals(lastEntiry)) {
          return constructObjectPathWithPrefix(volumeId, bucketId, dirInfo.getObjectID()) + OM_KEY_PREFIX;
        }

        // Check if the key exists
        long dirID = handler.getDirObjectId(names, names.length);
        String keyKey = constructObjectPathWithPrefix(volumeId, bucketId, dirID) +
            OM_KEY_PREFIX + lastEntiry;
        OmKeyInfo keyInfo = openFileTable.getSkipCache(keyKey);
        if (keyInfo != null && keyInfo.getFileName().equals(lastEntiry)) {
          return constructObjectPathWithPrefix(volumeId, bucketId,
              keyInfo.getParentObjectID()) + OM_KEY_PREFIX + lastEntiry;
        }

        return prevKeyPrefix;
      }
    } catch (IllegalArgumentException e) {
      log.error(
          "IllegalArgumentException encountered while converting key prefix to object path: {}",
          prevKeyPrefix, e);
      throw e;
    } catch (RuntimeException e) {
      log.error(
          "RuntimeException encountered while converting key prefix to object path: {}",
          prevKeyPrefix, e);
      return prevKeyPrefix;
    }
    return prevKeyPrefix;
  }

  /**
   * Make HTTP GET call on the URL and return HttpURLConnection instance.
   *
   * @param connectionFactory URLConnectionFactory to use.
   * @param url               url to call
   * @param isSpnego          is SPNEGO enabled
   * @return HttpURLConnection instance of the HTTP call.
   * @throws IOException While reading the response,
   * @throws AuthenticationException
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
    File lastKnownSnapshotFile = null;
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
              if (lastKnownSnapshotFile != null) {
                try {
                  FileUtils.forceDelete(lastKnownSnapshotFile);
                } catch (IOException e) {
                  log.warn("Error deleting existing om db snapshot directory: {}",
                      lastKnownSnapshotFile.getAbsolutePath());
                }
              }
              lastKnonwnSnapshotTs = snapshotTimestamp;
              lastKnownSnapshotFileName = fileName;
              lastKnownSnapshotFile = snapshotFile;
            }
          } catch (NumberFormatException nfEx) {
            log.warn("Unknown file found in Recon DB dir : {}", fileName);
            FileUtils.deleteQuietly(snapshotFile);
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
   * Converts Unix numeric permissions into a symbolic representation.
   * @param numericPermissions The numeric string, e.g., "750".
   * @return The symbolic representation, e.g., "rwxr-x---".
   */
  public static String convertNumericToSymbolic(String numericPermissions) {
    int owner = Character.getNumericValue(numericPermissions.charAt(0));
    int group = Character.getNumericValue(numericPermissions.charAt(1));
    int others = Character.getNumericValue(numericPermissions.charAt(2));

    return String.format("%s%s%s",
        convertToSymbolicPermission(owner),
        convertToSymbolicPermission(group),
        convertToSymbolicPermission(others));
  }

  /**
   * Converts a single digit Unix permission into a symbolic representation.
   * @param permission The permission digit.
   * @return The symbolic representation for the digit.
   */
  public static String convertToSymbolicPermission(int permission) {
    String[] symbols = {"---", "--x", "-w-", "-wx", "r--", "r-x", "rw-", "rwx"};
    return symbols[permission];
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

  /**
   * Return if all OMDB tables that will be used are initialized.
   * @return if tables are initialized
   */
  public static boolean isInitializationComplete(ReconOMMetadataManager omMetadataManager) {
    if (omMetadataManager == null) {
      return false;
    }
    return omMetadataManager.getVolumeTable() != null
        && omMetadataManager.getBucketTable() != null
        && omMetadataManager.getDirectoryTable() != null
        && omMetadataManager.getFileTable() != null
        && omMetadataManager.getKeyTable(BucketLayout.LEGACY) != null;
  }

  /**
   * Converts string date in a provided format to server timezone's epoch milllioseconds.
   *
   * @param dateString
   * @param dateFormat
   * @param timeZone
   * @return the epoch milliseconds representation of the date.
   */
  public static long convertToEpochMillis(String dateString, String dateFormat, TimeZone timeZone) {
    String localDateFormat = dateFormat;
    try {
      if (StringUtils.isEmpty(dateString)) {
        return Instant.now().toEpochMilli();
      }
      if (StringUtils.isEmpty(dateFormat)) {
        localDateFormat = "MM-dd-yyyy HH:mm:ss";
      }
      if (null == timeZone) {
        timeZone = TimeZone.getDefault();
      }
      SimpleDateFormat sdf = new SimpleDateFormat(localDateFormat);
      sdf.setTimeZone(timeZone); // Set server's timezone
      Date date = sdf.parse(dateString);
      return date.getTime(); // Convert to epoch milliseconds
    } catch (ParseException parseException) {
      log.error("Date parse exception for date: {} in format: {} -> {}", dateString, localDateFormat, parseException);
      return Instant.now().toEpochMilli();
    } catch (Exception exception) {
      log.error("Unexpected error while parsing date: {} in format: {} -> {}", dateString, localDateFormat, exception);
      return Instant.now().toEpochMilli();
    }
  }

  public static boolean validateStartPrefix(String startPrefix) {

    // Ensure startPrefix starts with '/' for non-empty values
    startPrefix = startPrefix.startsWith("/") ? startPrefix : "/" + startPrefix;

    // Split the path to ensure it's at least at the bucket level (volume/bucket).
    String[] pathComponents = startPrefix.split("/");
    if (pathComponents.length < 3 || pathComponents[2].isEmpty()) {
      return false; // Invalid if not at bucket level or deeper
    }

    return true;
  }

  /**
   * Retrieves keys from the specified table based on pagination and prefix filtering.
   * This method handles different scenarios based on the presence of {@code startPrefix}
   * and {@code prevKey}, enabling efficient key retrieval from the table.
   *
   * The method handles the following cases:
   *
   * 1. {@code prevKey} provided, {@code startPrefix} empty:
   *    - Seeks to {@code prevKey}, skips it, and returns subsequent records up to the limit.
   *
   * 2. {@code prevKey} empty, {@code startPrefix} empty:
   *    - Iterates from the beginning of the table, retrieving all records up to the limit.
   *
   * 3. {@code startPrefix} provided, {@code prevKey} empty:
   *    - Seeks to the first key matching {@code startPrefix} and returns all matching keys up to the limit.
   *
   * 4. {@code startPrefix} provided, {@code prevKey} provided:
   *    - Seeks to {@code prevKey}, skips it, and returns subsequent keys that match {@code startPrefix},
   *      up to the limit.
   *
   * This method also handles the following {@code limit} scenarios:
   * - If {@code limit == 0} or {@code limit < -1}, no records are returned.
   * - If {@code limit == -1}, all records are returned.
   * - For positive {@code limit}, it retrieves records up to the specified {@code limit}.
   *
   * @param table       The table to retrieve keys from.
   * @param startPrefix The search prefix to match keys against.
   * @param limit       The maximum number of keys to retrieve.
   * @param prevKey     The key to start after for the next set of records.
   * @return A map of keys and their corresponding {@code OmKeyInfo} or {@code RepeatedOmKeyInfo} objects.
   * @throws IOException If there are problems accessing the table.
   */
  public static <T> Map<String, T> extractKeysFromTable(
      Table<String, T> table, String startPrefix, int limit, String prevKey)
      throws IOException {

    Map<String, T> matchedKeys = new LinkedHashMap<>();

    // Null check for the table to prevent NPE during omMetaManager initialization
    if (table == null) {
      log.error("Table object is null. omMetaManager might still be initializing.");
      return Collections.emptyMap();
    }

    // If limit = 0, return an empty result set
    if (limit == 0 || limit < -1) {
      return matchedKeys;
    }

    // If limit = -1, set it to Integer.MAX_VALUE to return all records
    int actualLimit = (limit == -1) ? Integer.MAX_VALUE : limit;

    try (TableIterator<String, ? extends Table.KeyValue<String, T>> keyIter = table.iterator()) {

      // Scenario 1 & 4: prevKey is provided (whether startPrefix is empty or not)
      if (!prevKey.isEmpty()) {
        keyIter.seek(prevKey);
        if (keyIter.hasNext()) {
          keyIter.next();  // Skip the previous key record
        }
      } else if (!startPrefix.isEmpty()) {
        // Scenario 3: startPrefix is provided but prevKey is empty, so seek to startPrefix
        keyIter.seek(startPrefix);
      }

      // Scenario 2: Both startPrefix and prevKey are empty (iterate from the start of the table)
      // No seeking needed; just start iterating from the first record in the table
      // This is implicit in the following loop, as the iterator will start from the beginning

      // Iterate through the keys while adhering to the limit (if the limit is not zero)
      while (keyIter.hasNext() && matchedKeys.size() < actualLimit) {
        Table.KeyValue<String, T> entry = keyIter.next();
        String dbKey = entry.getKey();

        // Scenario 3 & 4: If startPrefix is provided, ensure the key matches startPrefix
        if (!startPrefix.isEmpty() && !dbKey.startsWith(startPrefix)) {
          break;  // If the key no longer matches the prefix, exit the loop
        }

        // Add the valid key-value pair to the results
        matchedKeys.put(dbKey, entry.getValue());
      }
    } catch (IOException exception) {
      log.error("Error retrieving keys from table for path: {}", startPrefix, exception);
      throw exception;
    }
    return matchedKeys;
  }

  /**
   * Finds all subdirectories under a parent directory in an FSO bucket. It builds
   * a list of paths for these subdirectories. These sub-directories are then used
   * to search for open files in the openFileTable.
   *
   * How it works:
   * - Starts from a parent directory identified by parentId.
   * - Looks through all child directories of this parent.
   * - For each child, it creates a path that starts with volumeID/bucketID/parentId,
   *   following our openFileTable format.
   * - Adds these paths to a list and explores each child further for more subdirectories.
   *
   * @param parentId The ID of the parent directory from which to start gathering subdirectories.
   * @param subPaths The list to which the paths of subdirectories will be added.
   * @param volumeID The ID of the volume containing the parent directory.
   * @param bucketID The ID of the bucket containing the parent directory.
   * @param reconNamespaceSummaryManager The manager used to retrieve NSSummary objects.
   * @throws IOException If an I/O error occurs while fetching NSSummary objects.
   */
  public static void gatherSubPaths(long parentId, List<String> subPaths,
                              long volumeID, long bucketID,
                              ReconNamespaceSummaryManager reconNamespaceSummaryManager)
      throws IOException {
    // Fetch the NSSummary object for parentId
    NSSummary parentSummary =
        reconNamespaceSummaryManager.getNSSummary(parentId);
    if (parentSummary == null) {
      return;
    }

    Set<Long> childDirIds = parentSummary.getChildDir();
    for (Long childId : childDirIds) {
      // Fetch the NSSummary for each child directory
      NSSummary childSummary =
          reconNamespaceSummaryManager.getNSSummary(childId);
      if (childSummary != null) {
        String subPath =
            ReconUtils.constructObjectPathWithPrefix(volumeID, bucketID,
                childId);
        // Add to subPaths
        subPaths.add(subPath);
        // Recurse into this child directory
        gatherSubPaths(childId, subPaths, volumeID, bucketID,
            reconNamespaceSummaryManager);
      }
    }
  }

  /**
   * Validates volume or bucket names according to specific rules.
   *
   * @param resName The name to validate (volume or bucket).
   * @return A Response object if validation fails, or null if the name is valid.
   */
  public static Response validateNames(String resName)
      throws IllegalArgumentException {
    if (resName.length() < OzoneConsts.OZONE_MIN_BUCKET_NAME_LENGTH ||
        resName.length() > OzoneConsts.OZONE_MAX_BUCKET_NAME_LENGTH) {
      throw new IllegalArgumentException(
          "Bucket or Volume name length should be between " +
              OzoneConsts.OZONE_MIN_BUCKET_NAME_LENGTH + " and " +
              OzoneConsts.OZONE_MAX_BUCKET_NAME_LENGTH);
    }

    if (resName.charAt(0) == '.' || resName.charAt(0) == '-' ||
        resName.charAt(resName.length() - 1) == '.' ||
        resName.charAt(resName.length() - 1) == '-') {
      throw new IllegalArgumentException(
          "Bucket or Volume name cannot start or end with " +
              "hyphen or period");
    }

    // Regex to check for lowercase letters, numbers, hyphens, underscores, and periods only.
    if (!resName.matches("^[a-z0-9._-]+$")) {
      throw new IllegalArgumentException(
          "Bucket or Volume name can only contain lowercase " +
              "letters, numbers, hyphens, underscores, and periods");
    }

    // If all checks pass, the name is valid
    return null;
  }

  /**
   * Constructs an object path with the given IDs.
   *
   * @param ids The IDs to construct the object path with.
   * @return The constructed object path.
   */
  public static String constructObjectPathWithPrefix(long... ids) {
    StringBuilder pathBuilder = new StringBuilder();
    for (long id : ids) {
      pathBuilder.append(OM_KEY_PREFIX).append(id);
    }
    return pathBuilder.toString();
  }

  public static Map<String, Object> getMetricsData(List<Map<String, Object>> metrics, String beanName) {
    if (metrics == null || StringUtils.isEmpty(beanName)) {
      return null;
    }
    for (Map<String, Object> item :metrics) {
      if (beanName.equals(item.get("name"))) {
        return item;
      }
    }
    return null;
  }

  public static long extractLongMetricValue(Map<String, Object> metrics, String keyName) {
    if (metrics == null || StringUtils.isEmpty(keyName)) {
      return  -1;
    }
    Object value = metrics.get(keyName);
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    if (value instanceof String) {
      try {
        return Long.parseLong((String) value);
      } catch (NumberFormatException e) {
        log.error("Failed to parse long value for key: {} with value: {}", keyName, value, e);
      }
    }
    return -1;
  }
}

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

package org.apache.hadoop.ozone.recon.api.handlers;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SIZE_CALC_PARALLEL_ENABLED;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SIZE_CALC_PARALLEL_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SIZE_CALC_PARALLEL_THRESHOLD;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SIZE_CALC_PARALLEL_THRESHOLD_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SIZE_CALC_PARALLEL_POOL_SIZE;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SIZE_CALC_PARALLEL_POOL_SIZE_DEFAULT;

import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.FileSizeDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.QuotaUsageResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Class for handling all entity types.
 */
public abstract class EntityHandler {

  private static final Logger LOG = LoggerFactory.getLogger(EntityHandler.class);

  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;

  private final ReconOMMetadataManager omMetadataManager;

  private final BucketHandler bucketHandler;

  private final OzoneStorageContainerManager reconSCM;

  private final String normalizedPath;
  private final String[] names;
  
  // Parallel size calculation components
  private final boolean parallelSizeCalculationEnabled;
  private final ParallelSizeCalculator parallelSizeCalculator;

  public EntityHandler(
          ReconNamespaceSummaryManager reconNamespaceSummaryManager,
          ReconOMMetadataManager omMetadataManager,
          OzoneStorageContainerManager reconSCM,
          BucketHandler bucketHandler, String path) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.omMetadataManager = omMetadataManager;
    this.reconSCM = reconSCM;
    this.bucketHandler = bucketHandler;

    // Initialize parallel size calculation
    OzoneConfiguration conf = omMetadataManager.getOzoneConfiguration();
    this.parallelSizeCalculationEnabled = conf.getBoolean(
        OZONE_RECON_SIZE_CALC_PARALLEL_ENABLED,
        OZONE_RECON_SIZE_CALC_PARALLEL_ENABLED_DEFAULT);
    
    if (parallelSizeCalculationEnabled) {
      int parallelism = conf.getInt(
          OZONE_RECON_SIZE_CALC_PARALLEL_POOL_SIZE,
          OZONE_RECON_SIZE_CALC_PARALLEL_POOL_SIZE_DEFAULT);
      int threshold = conf.getInt(
          OZONE_RECON_SIZE_CALC_PARALLEL_THRESHOLD,
          OZONE_RECON_SIZE_CALC_PARALLEL_THRESHOLD_DEFAULT);
      
      // Validate parallelism configuration
      if (parallelism <= 0) {
        LOG.warn("Invalid parallelism configuration: {}. Using default of available processors: {}", 
                 parallelism, OZONE_RECON_SIZE_CALC_PARALLEL_POOL_SIZE_DEFAULT);
        parallelism = OZONE_RECON_SIZE_CALC_PARALLEL_POOL_SIZE_DEFAULT;
      }
      
      this.parallelSizeCalculator = new ParallelSizeCalculator(
          reconNamespaceSummaryManager, parallelism, threshold);
      
      LOG.info("Parallel size calculation enabled with parallelism: {}, threshold: {}", 
               parallelism, threshold);
    } else {
      this.parallelSizeCalculator = null;
      LOG.info("Parallel size calculation disabled, using sequential approach");
    }

    // Defaulting to FILE_SYSTEM_OPTIMIZED if bucketHandler is null
    BucketLayout layout =
        (bucketHandler != null) ? bucketHandler.getBucketLayout() :
            BucketLayout.FILE_SYSTEM_OPTIMIZED;

    // Normalize the path based on the determined layout
    normalizedPath = normalizePath(path, layout);

    // Choose the parsing method based on the bucket layout
    names = (layout == BucketLayout.OBJECT_STORE) ?
        parseObjectStorePath(normalizedPath) : parseRequestPath(normalizedPath);
  }

  public abstract NamespaceSummaryResponse getSummaryResponse()
          throws IOException;

  public abstract DUResponse getDuResponse(
      boolean listFile, boolean withReplica, boolean sort)
          throws IOException;

  public abstract QuotaUsageResponse getQuotaResponse()
          throws IOException;

  public abstract FileSizeDistributionResponse getDistResponse()
          throws IOException;

  public ReconOMMetadataManager getOmMetadataManager() {
    return omMetadataManager;
  }

  public OzoneStorageContainerManager getReconSCM() {
    return reconSCM;
  }

  public ReconNamespaceSummaryManager getReconNamespaceSummaryManager() {
    return reconNamespaceSummaryManager;
  }

  public BucketHandler getBucketHandler() {
    return bucketHandler;
  }

  public String getNormalizedPath() {
    return normalizedPath;
  }

  public String[] getNames() {
    return names.clone();
  }

  /**
   * Return the entity handler of client's request, check path existence.
   * If path doesn't exist, return UnknownEntityHandler
   * @param reconNamespaceSummaryManager ReconNamespaceSummaryManager
   * @param omMetadataManager ReconOMMetadataManager
   * @param reconSCM OzoneStorageContainerManager
   * @param path the original path request used to identify root level
   * @return the entity handler of client's request
   */
  public static EntityHandler getEntityHandler(
          ReconNamespaceSummaryManager reconNamespaceSummaryManager,
          ReconOMMetadataManager omMetadataManager,
          OzoneStorageContainerManager reconSCM,
          String path) throws IOException {
    BucketHandler bucketHandler;

    String normalizedPath =
        normalizePath(path, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    String[] names = parseRequestPath(normalizedPath);
    if (path.equals(OM_KEY_PREFIX)) {
      return EntityType.ROOT.create(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, null, path);
    }

    if (names.length == 0) {
      return EntityType.UNKNOWN.create(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, null, path);
    } else if (names.length == 1) { // volume level check
      String volName = names[0];
      if (!omMetadataManager.volumeExists(volName)) {
        return EntityType.UNKNOWN.create(reconNamespaceSummaryManager,
                omMetadataManager, reconSCM, null, path);
      }
      return EntityType.VOLUME.create(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, null, path);
    } else if (names.length == 2) { // bucket level check
      String volName = names[0];
      String bucketName = names[1];

      bucketHandler = BucketHandler.getBucketHandler(
              reconNamespaceSummaryManager,
              omMetadataManager, reconSCM,
              volName, bucketName);

      if (bucketHandler == null
          || !bucketHandler.bucketExists(volName, bucketName)) {
        return EntityType.UNKNOWN.create(reconNamespaceSummaryManager,
                omMetadataManager, reconSCM, null, path);
      }
      return EntityType.BUCKET.create(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, bucketHandler, path);
    } else { // length > 3. check dir or key existence
      String volName = names[0];
      String bucketName = names[1];

      // Assuming getBucketHandler already validates volume and bucket existence
      bucketHandler = BucketHandler.getBucketHandler(
          reconNamespaceSummaryManager, omMetadataManager, reconSCM, volName,
          bucketName);

      if (bucketHandler == null) {
        return EntityType.UNKNOWN.create(reconNamespaceSummaryManager,
            omMetadataManager, reconSCM, null, path);
      }

      // Directly handle path normalization and parsing based on the layout
      if (bucketHandler.getBucketLayout() == BucketLayout.OBJECT_STORE) {
        String[] parsedObjectLayoutPath = parseObjectStorePath(
            normalizePath(path, bucketHandler.getBucketLayout()));
        if (parsedObjectLayoutPath == null) {
          return EntityType.UNKNOWN.create(reconNamespaceSummaryManager,
              omMetadataManager, reconSCM, null, path);
        }
        // Use the key part directly from the parsed path
        return bucketHandler.determineKeyPath(parsedObjectLayoutPath[2])
            .create(reconNamespaceSummaryManager, omMetadataManager, reconSCM,
                bucketHandler, path);
      } else {
        // Use the existing names array for non-OBJECT_STORE layouts to derive
        // the keyName
        String keyName = BucketHandler.getKeyName(names);
        return bucketHandler.determineKeyPath(keyName)
            .create(reconNamespaceSummaryManager, omMetadataManager, reconSCM,
                bucketHandler, path);
      }
    }
  }

  /**
   * Given an object ID, return the file size distribution.
   * @param objectId the object's ID
   * @return int array indicating file size distribution
   * @throws IOException ioEx
   */
  protected int[] getTotalFileSizeDist(long objectId) throws IOException {
    if (parallelSizeCalculationEnabled && parallelSizeCalculator != null) {
      try {
        return getTotalFileSizeDistParallel(objectId);
      } catch (Exception e) {
        LOG.warn("Parallel file size distribution calculation failed for objectId: {}, falling back to sequential", 
                 objectId, e);
        return getTotalFileSizeDistSequential(objectId);
      }
    } else {
      return getTotalFileSizeDistSequential(objectId);
    }
  }

  /**
   * Sequential implementation of getTotalFileSizeDist.
   */
  private int[] getTotalFileSizeDistSequential(long objectId) throws IOException {
    NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
    if (nsSummary == null) {
      return new int[ReconConstants.NUM_OF_FILE_SIZE_BINS];
    }
    int[] res = nsSummary.getFileSizeBucket();
    for (long childId: nsSummary.getChildDir()) {
      int[] subDirFileSizeDist = getTotalFileSizeDistSequential(childId);
      for (int i = 0; i < ReconConstants.NUM_OF_FILE_SIZE_BINS; ++i) {
        res[i] += subDirFileSizeDist[i];
      }
    }
    return res;
  }

  /**
   * Parallel implementation of getTotalFileSizeDist using ForkJoinPool.
   */
  private int[] getTotalFileSizeDistParallel(long objectId) throws IOException {
    FileSizeDistCalculationTask task = new FileSizeDistCalculationTask(objectId);
    return parallelSizeCalculator.getForkJoinPool().invoke(task);
  }

  /**
   * RecursiveTask for calculating file size distribution.
   */
  private class FileSizeDistCalculationTask extends java.util.concurrent.RecursiveTask<int[]> {
    private final long objectId;
    
    FileSizeDistCalculationTask(long objectId) {
      this.objectId = objectId;
    }
    
    @Override
    protected int[] compute() {
      try {
        NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
        if (nsSummary == null) {
          return new int[ReconConstants.NUM_OF_FILE_SIZE_BINS];
        }
        
        int[] res = nsSummary.getFileSizeBucket().clone();
        Set<Long> childDirs = nsSummary.getChildDir();
        
        if (childDirs.isEmpty()) {
          return res;
        }
        
        if (childDirs.size() >= parallelSizeCalculator.getParallelThreshold()) {
          // Parallel processing
          List<FileSizeDistCalculationTask> childTasks = new ArrayList<>();
          for (long childId : childDirs) {
            childTasks.add(new FileSizeDistCalculationTask(childId));
          }
          
          invokeAll(childTasks);
          
          for (FileSizeDistCalculationTask task : childTasks) {
            int[] childResult = task.join();
            for (int i = 0; i < ReconConstants.NUM_OF_FILE_SIZE_BINS; ++i) {
              res[i] += childResult[i];
            }
          }
        } else {
          // Sequential processing for small numbers of children
          for (long childId : childDirs) {
            int[] childResult = new FileSizeDistCalculationTask(childId).compute();
            for (int i = 0; i < ReconConstants.NUM_OF_FILE_SIZE_BINS; ++i) {
              res[i] += childResult[i];
            }
          }
        }
        
        return res;
      } catch (IOException e) {
        LOG.warn("Failed to calculate file size distribution for objectId: {}", objectId, e);
        return new int[ReconConstants.NUM_OF_FILE_SIZE_BINS];
      }
    }
  }

  protected int getTotalDirCount(long objectId) throws IOException {
    if (parallelSizeCalculationEnabled && parallelSizeCalculator != null) {
      try {
        return getTotalDirCountParallel(objectId);
      } catch (Exception e) {
        LOG.warn("Parallel directory count calculation failed for objectId: {}, falling back to sequential", 
                 objectId, e);
        return getTotalDirCountSequential(objectId);
      }
    } else {
      return getTotalDirCountSequential(objectId);
    }
  }

  /**
   * Sequential implementation of getTotalDirCount.
   */
  private int getTotalDirCountSequential(long objectId) throws IOException {
    NSSummary nsSummary =
        getReconNamespaceSummaryManager().getNSSummary(objectId);
    if (nsSummary == null) {
      return 0;
    }
    Set<Long> subdirs = nsSummary.getChildDir();
    int totalCnt = subdirs.size();
    for (long subdir : subdirs) {
      totalCnt += getTotalDirCountSequential(subdir);
    }
    return totalCnt;
  }

  /**
   * Parallel implementation of getTotalDirCount using ForkJoinPool.
   */
  private int getTotalDirCountParallel(long objectId) throws IOException {
    DirCountCalculationTask task = new DirCountCalculationTask(objectId);
    return parallelSizeCalculator.getForkJoinPool().invoke(task);
  }

  /**
   * RecursiveTask for calculating directory count.
   */
  private class DirCountCalculationTask extends java.util.concurrent.RecursiveTask<Integer> {
    private final long objectId;
    
    DirCountCalculationTask(long objectId) {
      this.objectId = objectId;
    }
    
    @Override
    protected Integer compute() {
      try {
        NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
        if (nsSummary == null) {
          return 0;
        }
        
        Set<Long> subdirs = nsSummary.getChildDir();
        int totalCnt = subdirs.size();
        
        if (subdirs.isEmpty()) {
          return totalCnt;
        }
        
        if (subdirs.size() >= parallelSizeCalculator.getParallelThreshold()) {
          // Parallel processing
          List<DirCountCalculationTask> childTasks = new ArrayList<>();
          for (long childId : subdirs) {
            childTasks.add(new DirCountCalculationTask(childId));
          }
          
          invokeAll(childTasks);
          
          for (DirCountCalculationTask task : childTasks) {
            totalCnt += task.join();
          }
        } else {
          // Sequential processing for small numbers of children
          for (long childId : subdirs) {
            totalCnt += new DirCountCalculationTask(childId).compute();
          }
        }
        
        return totalCnt;
      } catch (IOException e) {
        LOG.warn("Failed to calculate directory count for objectId: {}", objectId, e);
        return 0;
      }
    }
  }

  /**
   * Given an object ID, return total count of keys under this object.
   * @param objectId the object's ID
   * @return count of keys
   * @throws IOException ioEx
   */
  protected long getTotalKeyCount(long objectId) throws IOException {
    if (parallelSizeCalculationEnabled && parallelSizeCalculator != null) {
      try {
        return getTotalKeyCountParallel(objectId);
      } catch (Exception e) {
        LOG.warn("Parallel key count calculation failed for objectId: {}, falling back to sequential", 
                 objectId, e);
        return getTotalKeyCountSequential(objectId);
      }
    } else {
      return getTotalKeyCountSequential(objectId);
    }
  }

  /**
   * Sequential implementation of getTotalKeyCount.
   */
  private long getTotalKeyCountSequential(long objectId) throws IOException {
    NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
    if (nsSummary == null) {
      return 0L;
    }
    long totalCnt = nsSummary.getNumOfFiles();
    for (long childId: nsSummary.getChildDir()) {
      totalCnt += getTotalKeyCountSequential(childId);
    }
    return totalCnt;
  }

  /**
   * Parallel implementation of getTotalKeyCount using ForkJoinPool.
   */
  private long getTotalKeyCountParallel(long objectId) throws IOException {
    KeyCountCalculationTask task = new KeyCountCalculationTask(objectId);
    return parallelSizeCalculator.getForkJoinPool().invoke(task);
  }

  /**
   * RecursiveTask for calculating key count.
   */
  private class KeyCountCalculationTask extends java.util.concurrent.RecursiveTask<Long> {
    private final long objectId;
    
    KeyCountCalculationTask(long objectId) {
      this.objectId = objectId;
    }
    
    @Override
    protected Long compute() {
      try {
        NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
        if (nsSummary == null) {
          return 0L;
        }
        
        long totalCnt = nsSummary.getNumOfFiles();
        Set<Long> childDirs = nsSummary.getChildDir();
        
        if (childDirs.isEmpty()) {
          return totalCnt;
        }
        
        if (childDirs.size() >= parallelSizeCalculator.getParallelThreshold()) {
          // Parallel processing
          List<KeyCountCalculationTask> childTasks = new ArrayList<>();
          for (long childId : childDirs) {
            childTasks.add(new KeyCountCalculationTask(childId));
          }
          
          invokeAll(childTasks);
          
          for (KeyCountCalculationTask task : childTasks) {
            totalCnt += task.join();
          }
        } else {
          // Sequential processing for small numbers of children
          for (long childId : childDirs) {
            totalCnt += new KeyCountCalculationTask(childId).compute();
          }
        }
        
        return totalCnt;
      } catch (IOException e) {
        LOG.warn("Failed to calculate key count for objectId: {}", objectId, e);
        return 0L;
      }
    }
  }

  /**
   * Given an object ID, return total data size (no replication)
   * under this object.
   * @param objectId the object's ID
   * @return total used data size in bytes
   * @throws IOException ioEx
   */
  protected long getTotalSize(long objectId) throws IOException {
    if (parallelSizeCalculationEnabled && parallelSizeCalculator != null) {
      try {
        return parallelSizeCalculator.calculateTotalSize(objectId);
      } catch (Exception e) {
        LOG.warn("Parallel size calculation failed for objectId: {}, falling back to sequential", 
                 objectId, e);
        // Fall back to sequential calculation
        return getTotalSizeSequential(objectId);
      }
    } else {
      return getTotalSizeSequential(objectId);
    }
  }

  /**
   * Sequential implementation of getTotalSize for fallback or when parallel is disabled.
   * @param objectId the object's ID
   * @return total used data size in bytes
   * @throws IOException ioEx
   */
  private long getTotalSizeSequential(long objectId) throws IOException {
    NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
    if (nsSummary == null) {
      return 0L;
    }
    long totalSize = nsSummary.getSizeOfFiles();
    for (long childId: nsSummary.getChildDir()) {
      totalSize += getTotalSizeSequential(childId);
    }
    return totalSize;
  }

  /**
   * Get performance metrics from the last parallel calculation.
   * @return Performance metrics array [rocksDBQueries, tasksExecuted] or null if parallel disabled
   */
  public long[] getParallelSizeCalculationMetrics() {
    if (parallelSizeCalculator != null) {
      return parallelSizeCalculator.getPerformanceMetrics();
    }
    return null;
  }

  /**
   * Check if parallel size calculation is enabled.
   * @return true if parallel calculation is enabled
   */
  public boolean isParallelSizeCalculationEnabled() {
    return parallelSizeCalculationEnabled;
  }

  /**
   * Get the parallel calculator configuration details.
   * @return String describing the parallel calculator configuration
   */
  public String getParallelCalculatorInfo() {
    if (parallelSizeCalculator != null) {
      return String.format("Parallelism: %d, Threshold: %d", 
                           parallelSizeCalculator.getParallelism(),
                           parallelSizeCalculator.getParallelThreshold());
    }
    return "Parallel calculation disabled";
  }

  /**
   * Cleanup method to properly shutdown the parallel calculator.
   * Should be called when the handler is no longer needed.
   */
  public void cleanup() {
    if (parallelSizeCalculator != null) {
      parallelSizeCalculator.shutdown();
    }
  }

  public static String[] parseRequestPath(String path) {
    if (path.startsWith(OM_KEY_PREFIX)) {
      path = path.substring(1);
    }
    String[] names = path.split(OM_KEY_PREFIX);
    return names;
  }

  /**
   * Splits an object store path into volume, bucket, and key name components.
   *
   * This method parses a path of the format "/volumeName/bucketName/keyName",
   * including paths with additional '/' characters within the key name. It's
   * designed for object store paths where the first three '/' characters
   * separate the root, volume and bucket names from the key name.
   *
   * @param path The object store path to parse, starting with a slash.
   * @return A String array with three elements: volume name, bucket name, and
   * key name, or {null} if the path format is invalid.
   */
  public static String[] parseObjectStorePath(String path) {
    // Removing the leading slash for correct splitting
    path = path.substring(1);

    // Splitting the modified path by "/", limiting to 3 parts
    String[] parts = path.split("/", 3);

    // Checking if we correctly obtained 3 parts after removing the leading slash
    if (parts.length <= 3) {
      return parts;
    } else {
      return null;
    }
  }

  /**
   * Normalizes a given path based on the specified bucket layout.
   *
   * This method adjusts the path according to the bucket layout.
   * For {OBJECT_STORE Layout}, it normalizes the path up to the bucket level
   * using OmUtils.normalizePathUptoBucket. For other layouts, it
   * normalizes the entire path, including the key, using
   * OmUtils.normalizeKey, and does not preserve any trailing slashes.
   * The normalized path will always be prefixed with OM_KEY_PREFIX to ensure it
   * is consistent with the expected format for object storage paths in Ozone.
   *
   * @param path
   * @param bucketLayout
   * @return A normalized path
   */
  public static String normalizePath(String path, BucketLayout bucketLayout) {
    if (bucketLayout == BucketLayout.OBJECT_STORE) {
      return OM_KEY_PREFIX + OmUtils.normalizePathUptoBucket(path);
    }
    return OM_KEY_PREFIX + OmUtils.normalizeKey(path, false);
  }
}

package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.helpers.OzoneFSUtils.removeTrailingSlashIfNeeded;

abstract public class BucketHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      BucketHandler.class);

  protected ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  protected ReconOMMetadataManager omMetadataManager;

  protected ContainerManager containerManager;

  public BucketHandler(ReconNamespaceSummaryManager reconNamespaceSummaryManager,
                       ReconOMMetadataManager omMetadataManager,
                     OzoneStorageContainerManager reconSCM) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.omMetadataManager = omMetadataManager;
    this.containerManager = reconSCM.getContainerManager();
  }
    abstract public EntityType determineKeyPath(String keyName, long bucketObjectId) throws IOException;

    abstract public long calculateDUUnderObject(long parentId) throws IOException;

    abstract public long handleDirectKeys(long parentId, boolean withReplica,
                                          boolean listFile,
                                          List<DUResponse.DiskUsage> duData,
                                          String normalizedPath) throws IOException;

    abstract public long getDirObjectId(String[] names) throws IOException;

    abstract public long getDirObjectId(String[] names, int cutoff) throws IOException;

  /**
   *
   * @param path
   * @param nextLevel
   * @return
   */
  public static String buildSubpath(String path, String nextLevel) {
    String subpath = path;
    if (!subpath.startsWith(OM_KEY_PREFIX)) {
      subpath = OM_KEY_PREFIX + subpath;
    }
    subpath = removeTrailingSlashIfNeeded(subpath);
    if (nextLevel != null) {
      subpath = subpath + OM_KEY_PREFIX + nextLevel;
    }
    return subpath;
  }

  public long getKeySizeWithReplication(OmKeyInfo keyInfo) {
    OmKeyLocationInfoGroup locationGroup = keyInfo.getLatestVersionLocations();
    List<OmKeyLocationInfo> keyLocations =
        locationGroup.getBlocksLatestVersionOnly();
    long du = 0L;
    // a key could be too large to fit in one single container
    for (OmKeyLocationInfo location: keyLocations) {
      BlockID block = location.getBlockID();
      ContainerID containerId = new ContainerID(block.getContainerID());
      try {
        int replicationFactor =
            containerManager.getContainerReplicas(containerId).size();
        long blockSize = location.getLength() * replicationFactor;
        du += blockSize;
      } catch (ContainerNotFoundException cnfe) {
        LOG.warn("Cannot find container {}", block.getContainerID(), cnfe);
      }
    }
    return du;
  }

  /**
   * Example: /vol1/buck1/a/b/c/d/e/file1.txt -> a/b/c/d/e/file1.txt.
   * @param names parsed request
   * @return key name
   */
  public static String getKeyName(String[] names) {
    String[] keyArr = Arrays.copyOfRange(names, 2, names.length);
    return String.join(OM_KEY_PREFIX, keyArr);
  }

  protected boolean bucketExists(String volName, String bucketName)
      throws IOException {
    String bucketDBKey = omMetadataManager.getBucketKey(volName, bucketName);
    // Check if bucket exists
    return omMetadataManager.getBucketTable().getSkipCache(bucketDBKey) != null;
  }

  /**
   * Given a existent path, get the bucket object ID.
   * @param names valid path request
   * @return bucket objectID
   * @throws IOException
   */
  public long getBucketObjectId(String[] names) throws IOException {
    String bucketKey = omMetadataManager.getBucketKey(names[0], names[1]);
    OmBucketInfo bucketInfo = omMetadataManager
        .getBucketTable().getSkipCache(bucketKey);
    return bucketInfo.getObjectID();
  }

  // NOTE: In next PR, Will be updated for other bucket types depending on the path
  //  For now only FSO buckets supported
  static public BucketHandler getBucketHandler(String path,
                                               ReconNamespaceSummaryManager reconNamespaceSummaryManager,
    ReconOMMetadataManager omMetadataManager,
    OzoneStorageContainerManager reconSCM) {

    return new FSOBucketHandler(reconNamespaceSummaryManager, omMetadataManager, reconSCM);
    }
}

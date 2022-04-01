/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.recon.api.types;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.*;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.NSSummaryEndpoint;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.helpers.OzoneFSUtils.removeTrailingSlashIfNeeded;

public class EntityUtils {

    private static final Logger LOG = LoggerFactory.getLogger(
            NSSummaryEndpoint.class);

    private ReconNamespaceSummaryManager reconNamespaceSummaryManager;

    private ReconOMMetadataManager omMetadataManager;

    private ContainerManager containerManager;

    public EntityUtils(ReconNamespaceSummaryManager reconNamespaceSummaryManager,
                       ReconOMMetadataManager omMetadataManager,
                       OzoneStorageContainerManager reconSCM) {
        this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
        this.omMetadataManager = omMetadataManager;
        this.containerManager = reconSCM.getContainerManager();
    }

    public ReconOMMetadataManager getOmMetadataManager() {
        return omMetadataManager;
    }

    public ReconNamespaceSummaryManager getReconNamespaceSummaryManager() {
        return reconNamespaceSummaryManager;
    }

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

    protected long getKeySizeWithReplication(OmKeyInfo keyInfo) {
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
     * Return the entity type of client's request, check path existence.
     * If path doesn't exist, return Entity.UNKNOWN
     * @param path the original path request used to identify root level
     * @param names the client's parsed request
     * @return the entity type, unknown if path not found
     */
    @VisibleForTesting
    public EntityType getEntityType(String path, String[] names)
            throws IOException {
        if (path.equals(OM_KEY_PREFIX)) {
            return EntityType.ROOT;
        }

        if (names.length == 0) {
            return EntityType.UNKNOWN;
        } else if (names.length == 1) { // volume level check
            String volName = names[0];
            if (!volumeExists(volName)) {
                return EntityType.UNKNOWN;
            }
            return EntityType.VOLUME;
        } else if (names.length == 2) { // bucket level check
            String volName = names[0];
            String bucketName = names[1];
            if (!bucketExists(volName, bucketName)) {
                return EntityType.UNKNOWN;
            }
            return EntityType.BUCKET;
        } else { // length > 3. check dir or key existence (FSO-enabled)
            String volName = names[0];
            String bucketName = names[1];
            String keyName = getKeyName(names);
            // check if either volume or bucket doesn't exist
            if (!volumeExists(volName)
                    || !bucketExists(volName, bucketName)) {
                return EntityType.UNKNOWN;
            }
            long bucketObjectId = getBucketObjectId(names);
            return determineKeyPath(keyName, bucketObjectId);
        }
    }

    /**
     * Helper function to check if a path is a directory, key, or invalid.
     * @param keyName key name
     * @return DIRECTORY, KEY, or UNKNOWN
     * @throws IOException
     */
    protected EntityType determineKeyPath(String keyName, long bucketObjectId)
            throws IOException {

        java.nio.file.Path keyPath = Paths.get(keyName);
        Iterator<Path> elements = keyPath.iterator();

        long lastKnownParentId = bucketObjectId;
        OmDirectoryInfo omDirInfo = null;
        while (elements.hasNext()) {
            String fileName = elements.next().toString();

            // For example, /vol1/buck1/a/b/c/d/e/file1.txt
            // 1. Do lookup path component on directoryTable starting from bucket
            // 'buck1' to the leaf node component, which is 'file1.txt'.
            // 2. If there is no dir exists for the leaf node component 'file1.txt'
            // then do look it on fileTable.
            String dbNodeName = omMetadataManager.getOzonePathKey(
                    lastKnownParentId, fileName);
            omDirInfo = omMetadataManager.getDirectoryTable()
                    .getSkipCache(dbNodeName);

            if (omDirInfo != null) {
                lastKnownParentId = omDirInfo.getObjectID();
            } else if (!elements.hasNext()) {
                // reached last path component. Check file exists for the given path.
                OmKeyInfo omKeyInfo = omMetadataManager.getFileTable()
                        .getSkipCache(dbNodeName);
                // The path exists as a file
                if (omKeyInfo != null) {
                    omKeyInfo.setKeyName(keyName);
                    return EntityType.KEY;
                }
            } else {
                // Missing intermediate directory and just return null;
                // key not found in DB
                return EntityType.UNKNOWN;
            }
        }

        if (omDirInfo != null) {
            return EntityType.DIRECTORY;
        }
        return EntityType.UNKNOWN;
    }

    /**
     * Given an object ID, return the file size distribution.
     * @param objectId the object's ID
     * @return int array indicating file size distribution
     * @throws IOException ioEx
     */
    protected int[] getTotalFileSizeDist(long objectId) throws IOException {
        NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
        if (nsSummary == null) {
            return new int[ReconConstants.NUM_OF_BINS];
        }
        int[] res = nsSummary.getFileSizeBucket();
        for (long childId: nsSummary.getChildDir()) {
            int[] subDirFileSizeDist = getTotalFileSizeDist(childId);
            for (int i = 0; i < ReconConstants.NUM_OF_BINS; ++i) {
                res[i] += subDirFileSizeDist[i];
            }
        }
        return res;
    }

    /**
     * Return all volumes in the file system.
     * This method can be optimized by using username as a filter.
     * @return a list of volume names under the system
     */
    protected List<OmVolumeArgs> listVolumes() throws IOException {
        List<OmVolumeArgs> result = new ArrayList<>();
        Table volumeTable = omMetadataManager.getVolumeTable();
        TableIterator<String, ? extends Table.KeyValue<String, OmVolumeArgs>>
                iterator = volumeTable.iterator();

        while (iterator.hasNext()) {
            Table.KeyValue<String, OmVolumeArgs> kv = iterator.next();

            OmVolumeArgs omVolumeArgs = kv.getValue();
            if (omVolumeArgs != null) {
                result.add(omVolumeArgs);
            }
        }
        return result;
    }

    /**
     * List all buckets under a volume, if volume name is null, return all buckets
     * under the system.
     * @param volumeName volume name
     * @return a list of buckets
     * @throws IOException IOE
     */
    protected List<OmBucketInfo> listBucketsUnderVolume(final String volumeName)
            throws IOException {
        List<OmBucketInfo> result = new ArrayList<>();
        // if volume name is null, seek prefix is an empty string
        String seekPrefix = "";

        Table bucketTable = omMetadataManager.getBucketTable();

        TableIterator<String, ? extends Table.KeyValue<String, OmBucketInfo>>
                iterator = bucketTable.iterator();

        if (volumeName != null) {
            if (!volumeExists(volumeName)) {
                return result;
            }
            seekPrefix = omMetadataManager.getVolumeKey(volumeName + OM_KEY_PREFIX);
        }

        while (iterator.hasNext()) {
            Table.KeyValue<String, OmBucketInfo> kv = iterator.next();

            String key = kv.getKey();
            OmBucketInfo omBucketInfo = kv.getValue();

            if (omBucketInfo != null) {
                // We should return only the keys, whose keys match with the seek prefix
                if (key.startsWith(seekPrefix)) {
                    result.add(omBucketInfo);
                }
            }
        }
        return result;
    }

    protected boolean volumeExists(String volName) throws IOException {
        String volDBKey = omMetadataManager.getVolumeKey(volName);
        return omMetadataManager.getVolumeTable().getSkipCache(volDBKey) != null;
    }

    /**
     * Given an object ID, return total count of keys under this object.
     * @param objectId the object's ID
     * @return count of keys
     * @throws IOException ioEx
     */
    protected long getTotalKeyCount(long objectId) throws IOException {
        NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
        if (nsSummary == null) {
            return 0L;
        }
        long totalCnt = nsSummary.getNumOfFiles();
        for (long childId: nsSummary.getChildDir()) {
            totalCnt += getTotalKeyCount(childId);
        }
        return totalCnt;
    }

    /**
     * Given an object ID, return total count of directories under this object.
     * @param objectId the object's ID
     * @return count of directories
     * @throws IOException ioEx
     */
    protected int getTotalDirCount(long objectId) throws IOException {
        NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
        if (nsSummary == null) {
            return 0;
        }
        Set<Long> subdirs = nsSummary.getChildDir();
        int totalCnt = subdirs.size();
        for (long subdir: subdirs) {
            totalCnt += getTotalDirCount(subdir);
        }
        return totalCnt;
    }

    /**
     * Given a existent path, get the bucket object ID.
     * @param names valid path request
     * @return bucket objectID
     * @throws IOException
     */
    protected long getBucketObjectId(String[] names) throws IOException {
        String bucketKey = omMetadataManager.getBucketKey(names[0], names[1]);
        OmBucketInfo bucketInfo = omMetadataManager
                .getBucketTable().getSkipCache(bucketKey);
        return bucketInfo.getObjectID();
    }

    /**
     * Given a valid path request for a directory,
     * return the directory object ID.
     * @param names parsed path request in a list of names
     * @return directory object ID
     */
    protected long getDirObjectId(String[] names) throws IOException {
        return getDirObjectId(names, names.length);
    }

    /**
     * Given a valid path request and a cutoff length where should be iterated
     * up to.
     * return the directory object ID for the object at the cutoff length
     * @param names parsed path request in a list of names
     * @param cutoff cannot be larger than the names' length. If equals,
     *               return the directory object id for the whole path
     * @return directory object ID
     */
    protected long getDirObjectId(String[] names, int cutoff) throws IOException {
        long dirObjectId = getBucketObjectId(names);
        String dirKey = null;
        for (int i = 2; i < cutoff; ++i) {
            dirKey = omMetadataManager.getOzonePathKey(dirObjectId, names[i]);
            OmDirectoryInfo dirInfo =
                    omMetadataManager.getDirectoryTable().getSkipCache(dirKey);
            dirObjectId = dirInfo.getObjectID();
        }
        return dirObjectId;
    }

    /**
     * Given an object ID, return total data size (no replication)
     * under this object.
     * @param objectId the object's ID
     * @return total used data size in bytes
     * @throws IOException ioEx
     */
    protected long getTotalSize(long objectId) throws IOException {
        NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
        if (nsSummary == null) {
            return 0L;
        }
        long totalSize = nsSummary.getSizeOfFiles();
        for (long childId: nsSummary.getChildDir()) {
            totalSize += getTotalSize(childId);
        }
        return totalSize;
    }

    protected long calculateDUForVolume(String volumeName)
            throws IOException {
        long result = 0L;

        Table keyTable = omMetadataManager.getFileTable();

        TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
                iterator = keyTable.iterator();

        while (iterator.hasNext()) {
            Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
            OmKeyInfo keyInfo = kv.getValue();

            if (keyInfo != null) {
                if (volumeName.equals(keyInfo.getVolumeName())) {
                    result += getKeySizeWithReplication(keyInfo);
                }
            }
        }
        return result;
    }

    // FileTable's key is in the format of "parentId/fileName"
    // Make use of RocksDB's order to seek to the prefix and avoid full iteration
    protected long calculateDUUnderObject(long parentId) throws IOException {
        Table keyTable = omMetadataManager.getFileTable();

        TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
                iterator = keyTable.iterator();

        String seekPrefix = parentId + OM_KEY_PREFIX;
        iterator.seek(seekPrefix);
        long totalDU = 0L;
        // handle direct keys
        while (iterator.hasNext()) {
            Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
            String dbKey = kv.getKey();
            // since the RocksDB is ordered, seek until the prefix isn't matched
            if (!dbKey.startsWith(seekPrefix)) {
                break;
            }
            OmKeyInfo keyInfo = kv.getValue();
            if (keyInfo != null) {
                totalDU += getKeySizeWithReplication(keyInfo);
            }
        }

        // handle nested keys (DFS)
        NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(parentId);
        // empty bucket
        if (nsSummary == null) {
            return 0;
        }

        Set<Long> subDirIds = nsSummary.getChildDir();
        for (long subDirId: subDirIds) {
            totalDU += calculateDUUnderObject(subDirId);
        }
        return totalDU;
    }

    /**
     * This method handles disk usage of direct keys.
     * @param parentId parent directory/bucket
     * @param withReplica if withReplica is enabled, set sizeWithReplica
     * for each direct key's DU
     * @param listFile if listFile is enabled, append key DU as a subpath
     * @param duData the current DU data
     * @param normalizedPath the normalized path request
     * @return the total DU of all direct keys
     * @throws IOException IOE
     */
    protected long handleDirectKeys(long parentId, boolean withReplica,
                                  boolean listFile,
                                  List<DUResponse.DiskUsage> duData,
                                  String normalizedPath) throws IOException {

        Table keyTable = omMetadataManager.getFileTable();
        TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
                iterator = keyTable.iterator();

        String seekPrefix = parentId + OM_KEY_PREFIX;
        iterator.seek(seekPrefix);

        long keyDataSizeWithReplica = 0L;

        while (iterator.hasNext()) {
            Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
            String dbKey = kv.getKey();

            if (!dbKey.startsWith(seekPrefix)) {
                break;
            }
            OmKeyInfo keyInfo = kv.getValue();
            if (keyInfo != null) {
                DUResponse.DiskUsage diskUsage = new DUResponse.DiskUsage();
                String subpath = buildSubpath(normalizedPath,
                        keyInfo.getFileName());
                diskUsage.setSubpath(subpath);
                diskUsage.setKey(true);
                diskUsage.setSize(keyInfo.getDataSize());

                if (withReplica) {
                    long keyDU = getKeySizeWithReplication(keyInfo);
                    keyDataSizeWithReplica += keyDU;
                    diskUsage.setSizeWithReplica(keyDU);
                }
                // list the key as a subpath
                if (listFile) {
                    duData.add(diskUsage);
                }
            }
        }

        return keyDataSizeWithReplica;
    }
}

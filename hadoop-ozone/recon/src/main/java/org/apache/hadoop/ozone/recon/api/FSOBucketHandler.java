package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.EntityUtils;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

public class FSOBucketHandler extends BucketHandler {

    private EntityUtils entityUtils;

    private ReconOMMetadataManager omMetadataManager;

    private ReconNamespaceSummaryManager reconNamespaceSummaryManager;

    public FSOBucketHandler(EntityUtils entityUtils) {
        this.entityUtils = entityUtils;
        this.omMetadataManager = entityUtils.getOmMetadataManager();
        this.reconNamespaceSummaryManager = entityUtils.getReconNamespaceSummaryManager();
    }

    /**
     * Helper function to check if a path is a directory, key, or invalid.
     * @param keyName key name
     * @return DIRECTORY, KEY, or UNKNOWN
     * @throws IOException
     */
    @Override
    public EntityType determineKeyPath(String keyName, long bucketObjectId,
                                       BucketLayout bucketLayout) throws IOException {

        ReconOMMetadataManager omMetadataManager = entityUtils.getOmMetadataManager();
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


    // FileTable's key is in the format of "parentId/fileName"
    // Make use of RocksDB's order to seek to the prefix and avoid full iteration
    @Override
    public long calculateDUUnderObject(long parentId, BucketLayout bucketLayout) throws IOException {
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
                totalDU += entityUtils.getKeySizeWithReplication(keyInfo);
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
    @Override
    public long handleDirectKeys(long parentId, boolean withReplica,
                                    boolean listFile,
                                    List<DUResponse.DiskUsage> duData,
                                    String normalizedPath, BucketLayout bucketLayout) throws IOException {

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
                String subpath = EntityUtils.buildSubpath(normalizedPath,
                        keyInfo.getFileName());
                diskUsage.setSubpath(subpath);
                diskUsage.setKey(true);
                diskUsage.setSize(keyInfo.getDataSize());

                if (withReplica) {
                    long keyDU = entityUtils.getKeySizeWithReplication(keyInfo);
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

    /**
     * Given a valid path request for a directory,
     * return the directory object ID.
     * @param names parsed path request in a list of names
     * @return directory object ID
     */
    @Override
    public long getDirObjectId(String[] names, BucketLayout bucketLayout) throws IOException {
        return getDirObjectId(names, names.length,
                BucketLayout.FILE_SYSTEM_OPTIMIZED);
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
    @Override
    public long getDirObjectId(String[] names, int cutoff,
                               BucketLayout bucketLayout) throws IOException {
        long dirObjectId = entityUtils.getBucketObjectId(names);
        String dirKey = null;
        for (int i = 2; i < cutoff; ++i) {
            dirKey = omMetadataManager.getOzonePathKey(dirObjectId, names[i]);
            OmDirectoryInfo dirInfo =
                    omMetadataManager.getDirectoryTable().getSkipCache(dirKey);
            dirObjectId = dirInfo.getObjectID();
        }
        return dirObjectId;
    }

}

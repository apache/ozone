package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;

import java.io.IOException;
import java.util.List;

abstract public class BucketHandler {

    abstract public EntityType determineKeyPath(String keyName, long bucketObjectId,
                                                BucketLayout bucketLayout) throws IOException;

    abstract public long calculateDUUnderObject(long parentId,
                                                BucketLayout bucketLayout) throws IOException;

    abstract public long handleDirectKeys(long parentId, boolean withReplica,
                                          boolean listFile,
                                          List<DUResponse.DiskUsage> duData,
                                          String normalizedPath, BucketLayout bucketLayout) throws IOException;

    abstract public long getDirObjectId(String[] names,
                                        BucketLayout bucketLayout) throws IOException;

    abstract public long getDirObjectId(String[] names, int cutoff,
                                        BucketLayout bucketLayout) throws IOException;

}

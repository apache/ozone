package org.apache.hadoop.ozone.om.s3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.S3SecretStore;

import java.io.IOException;

/**
 * Implementation of provider with local S3 secret store.
 */
public class LocalS3StoreProvider implements S3SecretStoreProvider {
    private final OmMetadataManagerImpl omMetadataManager;

    public LocalS3StoreProvider(OmMetadataManagerImpl omMetadataManager) {
        this.omMetadataManager = omMetadataManager;
    }

    @Override
    public S3SecretStore get(Configuration conf) throws IOException {
        return omMetadataManager;
    }
}

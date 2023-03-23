package org.apache.hadoop.ozone.s3.remote;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.om.S3SecretStore;
import org.apache.hadoop.ozone.om.s3.S3SecretStoreProvider;
import org.apache.hadoop.ozone.s3.remote.vault.S3RemoteSecretStore;

import java.io.IOException;

public class S3VaultS3SecretStorage implements S3SecretStoreProvider {
    @Override
    public S3SecretStore get(Configuration conf) throws IOException {
        return S3RemoteSecretStore.fromConf(conf);
    }
}

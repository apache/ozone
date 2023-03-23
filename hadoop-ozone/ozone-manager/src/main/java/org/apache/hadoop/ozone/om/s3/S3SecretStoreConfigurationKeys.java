package org.apache.hadoop.ozone.om.s3;

/**
 * Configuration keys for S3 secret store and cache.
 */
public final class S3SecretStoreConfigurationKeys {
    private static final String PREFIX = "ozone.secret.s3.store.";

    public static final String S3_SECRET_STORAGE_TYPE = PREFIX + "provider";
    public static final Class<LocalS3StoreProvider> DEFAULT_SECRET_STORAGE_TYPE
            = LocalS3StoreProvider.class;

    //Cache configuration
    public static final String CACHE_PREFIX = PREFIX + "cache.";

    public static final String CACHE_LIFETIME = CACHE_PREFIX + "expireTime";
    public static final long DEFAULT_CACHE_LIFETIME = 600;

    public static final String CACHE_MAX_SIZE = CACHE_PREFIX + "capacity";
    public static final long DEFAULT_CACHE_MAX_SIZE = Long.MAX_VALUE;

    /**
     * Never constructed.
     */
    private S3SecretStoreConfigurationKeys() {

    }
}

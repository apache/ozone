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

package org.apache.hadoop.ozone.s3.endpoint;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_DATASTREAM_AUTO_THRESHOLD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_DATASTREAM_AUTO_THRESHOLD_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.ETAG;
import static org.apache.hadoop.ozone.OzoneConsts.KB;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_CLIENT_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_CLIENT_BUFFER_SIZE_KEY;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_ARGUMENT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_REQUEST;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_TAG;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;
import static org.apache.hadoop.ozone.s3.util.S3Consts.AWS_TAG_PREFIX;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_HEADER_PREFIX;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CONFIG_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_KEY_LENGTH_LIMIT;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_NUM_LIMIT;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_REGEX_PATTERN;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_VALUE_LENGTH_LIMIT;
import static org.apache.hadoop.ozone.s3.util.S3Utils.hasMultiChunksPayload;
import static org.apache.hadoop.ozone.s3.util.S3Utils.hasUnsignedPayload;
import static org.apache.hadoop.ozone.s3.util.S3Utils.urlDecode;
import static org.apache.hadoop.ozone.s3.util.S3Utils.validateMultiChunksUpload;
import static org.apache.hadoop.ozone.s3.util.S3Utils.validateSignatureHeader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import net.jcip.annotations.Immutable;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLogger.PerformanceStringBuilder;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.s3.MultiDigestInputStream;
import org.apache.hadoop.ozone.s3.RequestIdentifier;
import org.apache.hadoop.ozone.s3.SignedChunksInputStream;
import org.apache.hadoop.ozone.s3.UnsignedChunksInputStream;
import org.apache.hadoop.ozone.s3.commontypes.RequestParameters;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.metrics.S3GatewayMetrics;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.apache.hadoop.ozone.s3.util.AuditUtils;
import org.apache.hadoop.ozone.s3.util.S3Utils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.ratis.util.function.CheckedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic helpers for all the REST endpoints.
 */
public abstract class EndpointBase {

  protected static final String ETAG_CUSTOM = "etag-custom";
  protected static final String CONTENT_TYPE_CUSTOM = "content-type-custom";

  // System metadata key -> custom key. A user x-amz-meta-{etag,content-type}
  // collides with the system ETag / Content-Type stored under the same key, so
  // it is remapped on write and rebuilt on read; the system value is returned
  // via its own ETag / Content-Type response header.
  private static final Map<String, String> RESERVED_METADATA_KEYS =
      ImmutableMap.of(
          ETAG, ETAG_CUSTOM,
          HttpHeaders.CONTENT_TYPE, CONTENT_TYPE_CUSTOM);

  // Custom key -> lower-cased header, to rebuild remapped user metadata on read.
  private static final Map<String, String> REBUILT_RESERVED_KEYS =
      RESERVED_METADATA_KEYS.entrySet().stream()
          .collect(ImmutableMap.toImmutableMap(
              Map.Entry::getValue, e -> e.getKey().toLowerCase()));

  private static final ThreadLocal<MessageDigest> MD5_PROVIDER;
  private static final ThreadLocal<MessageDigest> SHA_256_PROVIDER;

  static {
    MD5_PROVIDER = ThreadLocal.withInitial(() -> {
      try {
        return MessageDigest.getInstance(OzoneConsts.MD5_HASH);
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    });

    SHA_256_PROVIDER = ThreadLocal.withInitial(() -> {
      try {
        return MessageDigest.getInstance(OzoneConsts.FILE_HASH);
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Inject
  private OzoneConfiguration ozoneConfiguration;

  @Inject
  private OzoneClient client;
  @SuppressWarnings("checkstyle:VisibilityModifier")
  @Inject
  protected SignatureInfo signatureInfo;
  @Inject
  private RequestIdentifier requestIdentifier;

  private S3Auth s3Auth;
  private int bufferSize;
  private int chunkSize;
  private boolean datastreamEnabled;
  private long datastreamMinLength;

  @Context
  private ContainerRequestContext context;

  @Context
  private HttpHeaders headers;

  // initialized in @PostConstruct
  private RequestParameters.MultivaluedMapImpl queryParams;

  private final Set<String> excludeMetadataFields =
      new HashSet<>(Arrays.asList(OzoneConsts.GDPR_FLAG, STORAGE_CONFIG_HEADER));
  private static final Logger LOG =
      LoggerFactory.getLogger(EndpointBase.class);

  protected static final AuditLogger AUDIT =
      new AuditLogger(AuditLoggerType.S3GLOGGER);

  /** Read-only access to query parameters. */
  protected RequestParameters queryParams() {
    return queryParams;
  }

  /** For setting multiple values use {@link #getContext()}. */
  public RequestParameters.Mutable queryParamsForTest() {
    return queryParams;
  }

  /**
   * Initializes the object post construction. Calls init() from any
   * child classes to work around the issue of only one method can be annotated.
   */
  @PostConstruct
  public void initialization() {
    queryParams = RequestParameters.of(context.getUriInfo().getQueryParameters());
    // Note: userPrincipal is initialized to be the same value as accessId,
    //  could be updated later in RpcClient#getS3Volume
    s3Auth = new S3Auth(signatureInfo.getStringToSign(),
        signatureInfo.getSignature(),
        signatureInfo.getAwsAccessId(), signatureInfo.getAwsAccessId());
    LOG.debug("S3 access id: {}", s3Auth.getAccessID());
    ClientProtocol clientProtocol =
        getClient().getObjectStore().getClientProxy();
    clientProtocol.setThreadLocalS3Auth(s3Auth);

    bufferSize = (int) getOzoneConfiguration().getStorageSize(
        OZONE_S3G_CLIENT_BUFFER_SIZE_KEY,
        OZONE_S3G_CLIENT_BUFFER_SIZE_DEFAULT, StorageUnit.BYTES);
    chunkSize = (int) getOzoneConfiguration().getStorageSize(
        OZONE_SCM_CHUNK_SIZE_KEY,
        OZONE_SCM_CHUNK_SIZE_DEFAULT,
        StorageUnit.BYTES);
    datastreamEnabled = getOzoneConfiguration().getBoolean(
        HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED,
        HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED_DEFAULT);
    datastreamMinLength = (long) getOzoneConfiguration().getStorageSize(
        OZONE_FS_DATASTREAM_AUTO_THRESHOLD,
        OZONE_FS_DATASTREAM_AUTO_THRESHOLD_DEFAULT, StorageUnit.BYTES);

    init();
  }

  protected void init() {
    // hook method
  }

  protected OzoneVolume getVolume() throws IOException {
    return client.getObjectStore().getS3Volume();
  }

  /**
   * Returns Iterator to iterate over all buckets for a specific user.
   * The result can be restricted using bucket prefix, will return all
   * buckets if bucket prefix is null.
   *
   * @param prefix Bucket prefix to match
   * @param volumeProcessor Volume processor to operate on volume
   * @return {@code Iterator<OzoneBucket>}
   */
  protected Iterator<? extends OzoneBucket> listS3Buckets(String prefix, Consumer<OzoneVolume> volumeProcessor)
      throws IOException, OS3Exception {
    return iterateBuckets(volume -> volume.listBuckets(prefix), volumeProcessor);
  }

  /**
   * Returns Iterator to iterate over all buckets after prevBucket for a
   * specific user. If prevBucket is null it returns an iterator to iterate
   * over all buckets for this user. The result can be restricted using
   * bucket prefix, will return all buckets if bucket prefix is null.
   *
   * @param prefix Bucket prefix to match
   * @param previousBucket Buckets are listed after this bucket
   * @return {@code Iterator<OzoneBucket>}
   */
  protected Iterator<? extends OzoneBucket> listS3Buckets(String prefix,
      String previousBucket, Consumer<OzoneVolume> volumeProcessor) throws IOException, OS3Exception {
    return iterateBuckets(volume -> volume.listBuckets(prefix, previousBucket), volumeProcessor);
  }

  private Iterator<? extends OzoneBucket> iterateBuckets(
      Function<OzoneVolume, Iterator<? extends OzoneBucket>> query,
      Consumer<OzoneVolume> ownerSetter)
      throws IOException, OS3Exception {
    try {
      OzoneVolume volume = getVolume();
      ownerSetter.accept(volume);
      return query.apply(volume);
    } catch (OMException e) {
      if (e.getResult() == ResultCodes.VOLUME_NOT_FOUND) {
        return Collections.emptyIterator();
      } else  if (e.getResult() == ResultCodes.PERMISSION_DENIED) {
        throw newError(S3ErrorTable.ACCESS_DENIED,
            "listBuckets", e);
      } else if (e.getResult() == ResultCodes.INVALID_TOKEN) {
        throw newError(S3ErrorTable.ACCESS_DENIED,
            s3Auth.getAccessID(), e);
      } else if (e.getResult() == ResultCodes.TIMEOUT ||
          e.getResult() == ResultCodes.INTERNAL_ERROR) {
        throw newError(S3ErrorTable.INTERNAL_ERROR,
            "listBuckets", e);
      } else {
        throw e;
      }
    }
  }

  protected Map<String, String> getCustomMetadataFromHeaders(
      MultivaluedMap<String, String> requestHeaders) throws OS3Exception {
    Map<String, String> customMetadata = new HashMap<>();
    if (requestHeaders == null || requestHeaders.isEmpty()) {
      return customMetadata;
    }

    Set<String> customMetadataKeys = requestHeaders.keySet().stream()
            .filter(k -> {
              if (k.toLowerCase().startsWith(CUSTOM_METADATA_HEADER_PREFIX) &&
                  !excludeMetadataFields.contains(
                        k.substring(
                          CUSTOM_METADATA_HEADER_PREFIX.length()))) {
                return true;
              }
              return false;
            })
            .collect(Collectors.toSet());

    long sizeInBytes = 0;
    if (!customMetadataKeys.isEmpty()) {
      for (String key : customMetadataKeys) {
        String mapKey =
            key.substring(CUSTOM_METADATA_HEADER_PREFIX.length());
        List<String> values = requestHeaders.get(key);
        String value = StringUtils.join(values, ",");
        sizeInBytes += mapKey.getBytes(UTF_8).length;
        sizeInBytes += value.getBytes(UTF_8).length;

        if (sizeInBytes >
                OzoneConsts.S3_REQUEST_HEADER_METADATA_SIZE_LIMIT_KB * KB) {
          throw newError(S3ErrorTable.METADATA_TOO_LARGE, key);
        }
        customMetadata.put(mapKey, value);
      }
    }

    // Remap user values that collide with a reserved system key.
    RESERVED_METADATA_KEYS.forEach((headerName, customKey) ->
        remapReservedMetadataKey(customMetadata, headerName, customKey));

    return customMetadata;
  }

  /**
   * Move a user value under {@code headerName} (canonical or lower-case) to
   * {@code customKey} so it does not collide with the system value.
   */
  private static void remapReservedMetadataKey(Map<String, String> metadata,
      String headerName, String customKey) {
    String lowerName = headerName.toLowerCase();
    if (metadata.containsKey(headerName) || metadata.containsKey(lowerName)) {
      String value = metadata.get(headerName) != null
          ? metadata.get(headerName) : metadata.get(lowerName);
      metadata.remove(headerName);
      metadata.remove(lowerName);
      metadata.put(customKey, value);
    }
  }

  protected void addCustomMetadataHeaders(
      Response.ResponseBuilder responseBuilder, OzoneKey key) {

    Map<String, String> metadata = key.getMetadata();
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      String metadataKey = entry.getKey();
      // System ETag / Content-Type are returned via their own response header.
      if (RESERVED_METADATA_KEYS.containsKey(metadataKey)) {
        continue;
      }
      // Rebuild a remapped user value (e.g. content-type-custom -> content-type).
      metadataKey = REBUILT_RESERVED_KEYS.getOrDefault(metadataKey, metadataKey);
      responseBuilder
          .header(CUSTOM_METADATA_HEADER_PREFIX + metadataKey,
              entry.getValue());
    }
  }

  protected Map<String, String> getTaggingFromHeaders(HttpHeaders httpHeaders)
      throws OS3Exception {
    String tagString = httpHeaders.getHeaderString(TAG_HEADER);

    if (StringUtils.isEmpty(tagString)) {
      return Collections.emptyMap();
    }

    List<NameValuePair> tagPairs = URLEncodedUtils.parse(tagString, UTF_8);

    // Put Object with x-amz-tagging header. A segment with no '=' (e.g."foo=bar&bar") is
    // typically represented as (key=bar, value=null). AWS S3 treats that as an empty value for "bar".
    // We map null → "" here only for this header path.
    // PutObjectTagging is different: the XML/JSON API requires each Tag to
    // include a Value element; so a missing Value stays null and fails validation.
    return validateAndGetTagging(tagPairs, NameValuePair::getName,
        pair -> {
          String v = pair.getValue();
          return v != null ? v : "";
        });
  }

  protected static <KV> Map<String, String> validateAndGetTagging(
      List<KV> tagList,
      Function<KV, String> getTagKey,
      Function<KV, String> getTagValue
  ) throws OS3Exception {
    return validateAndGetTagging(tagList, getTagKey, getTagValue, TAG_NUM_LIMIT);
  }

  protected static <KV> Map<String, String> validateAndGetTagging(
      List<KV> tagList,
      Function<KV, String> getTagKey,
      Function<KV, String> getTagValue,
      int maxTagCount
  ) throws OS3Exception {
    final Map<String, String> tags = new HashMap<>();
    for (KV tagPair : tagList) {
      final String tagKey = getTagKey.apply(tagPair);
      final String tagValue = getTagValue.apply(tagPair);
      // Tag restrictions: https://docs.aws.amazon.com/AmazonS3/latest/API/API_control_S3Tag.html
      if (StringUtils.isEmpty(tagKey)) {
        OS3Exception ex = S3ErrorTable.newError(INVALID_TAG, TAG_HEADER);
        ex.setErrorMessage("Some tag keys are empty, please only specify non-empty tag keys");
        throw ex;
      }

      if (StringUtils.startsWith(tagKey, AWS_TAG_PREFIX)) {
        OS3Exception ex = S3ErrorTable.newError(INVALID_TAG, tagKey);
        ex.setErrorMessage("Tag key cannot start with \"aws:\" prefix");
        throw ex;
      }

      if (tagValue == null) {
        // Missing tag value is invalid for PutObjectTagging XML/JSON; x-amz-tagging must
        // normalize null to "" in getTaggingFromHeaders before calling this method.
        OS3Exception ex = S3ErrorTable.newError(INVALID_TAG, tagKey);
        ex.setErrorMessage("Some tag values are not specified, please specify the tag values");
        throw ex;
      }

      if (tagKey.length() > TAG_KEY_LENGTH_LIMIT) {
        OS3Exception ex = S3ErrorTable.newError(INVALID_TAG, tagKey);
        ex.setErrorMessage("The tag key exceeds the maximum length of " + TAG_KEY_LENGTH_LIMIT);
        throw ex;
      }

      if (tagValue.length() > TAG_VALUE_LENGTH_LIMIT) {
        OS3Exception ex = S3ErrorTable.newError(INVALID_TAG, tagValue);
        ex.setErrorMessage("The tag value exceeds the maximum length of " + TAG_VALUE_LENGTH_LIMIT);
        throw ex;
      }

      if (!TAG_REGEX_PATTERN.matcher(tagKey).matches()) {
        OS3Exception ex = S3ErrorTable.newError(INVALID_TAG, tagKey);
        ex.setErrorMessage("The tag key does not have a valid pattern");
        throw ex;
      }

      if (!TAG_REGEX_PATTERN.matcher(tagValue).matches()) {
        OS3Exception ex = S3ErrorTable.newError(INVALID_TAG, tagValue);
        ex.setErrorMessage("The tag value does not have a valid pattern");
        throw ex;
      }

      final String previous = tags.put(tagKey, tagValue);
      if (previous != null) {
        // Tags must have unique keys on the same resource (object or bucket).
        OS3Exception ex = S3ErrorTable.newError(INVALID_TAG, tagKey);
        ex.setErrorMessage("There are tags with duplicate tag keys, tag keys should be unique");
        throw ex;
      }
    }

    if (tags.size() > maxTagCount) {
      // You can associate up to 10 tags with an object and up to 50 tags with a bucket.
      OS3Exception ex = S3ErrorTable.newError(INVALID_TAG, TAG_HEADER);
      ex.setErrorMessage("The number of tags " + tags.size() +
          " exceeded the maximum number of tags of " + maxTagCount);
      throw ex;
    }

    return Collections.unmodifiableMap(tags);
  }

  protected AuditMessage.Builder auditMessageFor(AuditAction op) {
    Map<String, String> auditMap = getAuditParameters();
    auditMap.put("x-amz-request-id", requestIdentifier.getRequestId());
    auditMap.put("x-amz-id-2", requestIdentifier.getAmzId());

    AuditMessage.Builder builder = new AuditMessage.Builder()
        .forOperation(op)
        .withParams(auditMap);
    if (s3Auth != null &&
        s3Auth.getAccessID() != null &&
        !s3Auth.getAccessID().isEmpty()) {
      builder.setUser(s3Auth.getAccessID());
    }
    if (context != null) {
      builder.atIp(AuditUtils.getClientIpAddress(context));
    }
    return builder;
  }

  protected AuditMessage.Builder auditMessageForSuccess(AuditAction op) {
    return auditMessageFor(op)
        .withResult(AuditEventStatus.SUCCESS);
  }

  protected AuditMessage.Builder auditMessageForFailure(AuditAction op, Throwable throwable) {
    return auditMessageFor(op)
        .withResult(AuditEventStatus.FAILURE)
        .withException(throwable);
  }

  @VisibleForTesting
  public void setClient(OzoneClient ozoneClient) {
    this.client = ozoneClient;
  }

  protected ContainerRequestContext getContext() {
    return context;
  }

  void setContext(ContainerRequestContext context) {
    this.context = context;
  }

  protected HttpHeaders getHeaders() {
    return headers;
  }

  void setHeaders(HttpHeaders headers) {
    this.headers = headers;
  }

  @VisibleForTesting
  public void setRequestIdentifier(RequestIdentifier requestIdentifier) {
    this.requestIdentifier = requestIdentifier;
  }

  @VisibleForTesting
  public void setSignatureInfo(SignatureInfo signatureInfo) {
    this.signatureInfo = signatureInfo;
  }

  public OzoneClient getClient() {
    return client;
  }

  protected ClientProtocol getClientProtocol() {
    return getClient().getProxy();
  }

  void setOzoneConfiguration(OzoneConfiguration conf) {
    ozoneConfiguration = conf;
  }

  /**
   * Copy dependencies from this endpoint to another endpoint.
   * Used for initializing handler instances.
   */
  protected void copyDependenciesTo(EndpointBase target) {
    if (this == target) {
      return;
    }
    target.queryParams = queryParams;
    target.s3Auth = s3Auth;
    target.setClient(this.client);
    target.setOzoneConfiguration(this.ozoneConfiguration);
    target.setContext(this.context);
    target.setHeaders(this.headers);
    target.setRequestIdentifier(this.requestIdentifier);
    target.setSignatureInfo(this.signatureInfo);
    target.init();
  }

  protected OzoneConfiguration getOzoneConfiguration() {
    return ozoneConfiguration;
  }

  @VisibleForTesting
  public S3GatewayMetrics getMetrics() {
    return S3GatewayMetrics.getMetrics();
  }

  protected Map<String, String> getAuditParameters() {
    return AuditUtils.getAuditParameters(context);
  }

  protected void auditWriteSuccess(AuditAction action, PerformanceStringBuilder perf) {
    AUDIT.logWriteSuccess(auditMessageForSuccess(action).setPerformance(perf).build());
  }

  protected void auditWriteSuccess(AuditAction action) {
    AUDIT.logWriteSuccess(auditMessageForSuccess(action).build());
  }

  protected void auditReadSuccess(AuditAction action, PerformanceStringBuilder perf) {
    AUDIT.logReadSuccess(auditMessageForSuccess(action).setPerformance(perf).build());
  }

  protected void auditReadSuccess(AuditAction action) {
    AUDIT.logReadSuccess(auditMessageForSuccess(action).build());
  }

  protected void auditWriteFailure(AuditAction action, Throwable ex) {
    AUDIT.logWriteFailure(auditMessageForFailure(action, ex).build());
  }

  protected void auditReadFailure(AuditAction action, Exception ex) {
    AUDIT.logReadFailure(auditMessageForFailure(action, ex).build());
  }

  protected static boolean isAccessDenied(OMException ex) {
    ResultCodes result = ex.getResult();
    return result == ResultCodes.PERMISSION_DENIED
        || result == ResultCodes.INVALID_TOKEN;
  }

  protected ReplicationConfig getReplicationConfig(OzoneBucket ozoneBucket) throws OS3Exception {
    String storageType = getHeaders().getHeaderString(STORAGE_CLASS_HEADER);
    String storageConfig = getHeaders().getHeaderString(CUSTOM_METADATA_HEADER_PREFIX + STORAGE_CONFIG_HEADER);

    ReplicationConfig clientConfiguredReplicationConfig =
        OzoneClientUtils.getClientConfiguredReplicationConfig(getOzoneConfiguration());

    return S3Utils.resolveS3ClientSideReplicationConfig(storageType, storageConfig,
        clientConfiguredReplicationConfig, ozoneBucket.getReplicationConfig());
  }

  /**
   * Parse the key and bucket name from copy header.
   */
  public static Pair<String, String> parseSourceHeader(String copyHeader)
      throws OS3Exception {
    String header = copyHeader;
    if (header.startsWith("/")) {
      header = copyHeader.substring(1);
    }
    int pos = header.indexOf('/');
    if (pos == -1) {
      OS3Exception ex = newError(INVALID_ARGUMENT, header);
      ex.setErrorMessage("Copy Source must mention the source bucket and " +
          "key: sourcebucket/sourcekey");
      throw ex;
    }

    try {
      String bucket = header.substring(0, pos);
      String key = urlDecode(header.substring(pos + 1));
      return Pair.of(bucket, key);
    } catch (UnsupportedEncodingException e) {
      OS3Exception ex = newError(INVALID_ARGUMENT, header, e);
      ex.setErrorMessage("Copy Source header could not be url-decoded");
      throw ex;
    }
  }

  protected static int parsePartNumberMarker(String partNumberMarker) {
    int partMarker = 0;
    if (partNumberMarker != null) {
      partMarker = Integer.parseInt(partNumberMarker);
    }
    return partMarker;
  }

  /**
   * Create a {@link S3ChunkInputStreamInfo} that contains the necessary information to handle
   * the S3 chunk upload.
   */
  protected S3ChunkInputStreamInfo getS3ChunkInputStreamInfo(
      InputStream body, long contentLength, String amzDecodedLength, String keyPath) throws OS3Exception {
    final String amzContentSha256Header = validateSignatureHeader(getHeaders(), keyPath, signatureInfo.isSignPayload());
    final InputStream chunkInputStream;
    final long effectiveLength;
    if (hasMultiChunksPayload(amzContentSha256Header)) {
      validateMultiChunksUpload(getHeaders(), amzDecodedLength, keyPath);
      if (hasUnsignedPayload(amzContentSha256Header)) {
        chunkInputStream = new UnsignedChunksInputStream(body);
      } else {
        chunkInputStream = new SignedChunksInputStream(body);
      }
      effectiveLength = Long.parseLong(amzDecodedLength);
    } else {
      // Single chunk upload: https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
      // Possible x-amz-content-sha256 header values
      // - Actual payload checksum value: For signed payload
      // - UNSIGNED-PAYLOAD: For unsigned payload
      chunkInputStream = body;
      effectiveLength = contentLength;
    }

    // MD5MessageDigest is used for ETag calculation
    // and Sha256Digest is used for "x-amz-content-sha256" header verification
    List<MessageDigest> digests = new ArrayList<>();
    digests.add(getMD5DigestInstance());
    if (!hasUnsignedPayload(amzContentSha256Header) && !hasMultiChunksPayload(amzContentSha256Header)) {
      digests.add(getSha256DigestInstance());
    }
    MultiDigestInputStream multiDigestInputStream =
        new MultiDigestInputStream(chunkInputStream, digests);
    return new S3ChunkInputStreamInfo(multiDigestInputStream, effectiveLength);
  }

  public boolean isDatastreamEnabled() {
    return datastreamEnabled;
  }

  public long getDatastreamMinLength() {
    return datastreamMinLength;
  }

  public int getChunkSize() {
    return chunkSize;
  }

  public static MessageDigest getMD5DigestInstance() {
    return MD5_PROVIDER.get();
  }

  public static MessageDigest getSha256DigestInstance() {
    return SHA_256_PROVIDER.get();
  }

  protected static CheckedRunnable<IOException> validateContentLength(
      long expectedLength, long actualLength, String keyPath) {
    return () -> {
      if (actualLength != expectedLength) {
        OS3Exception ex = newError(INVALID_REQUEST, keyPath);
        ex.setErrorMessage(String.format(
            "Request body length %d does not match expected length %d",
            actualLength, expectedLength));
        throw ex;
      }
    };
  }

  protected static String extractPartsCount(String eTag) {
    if (eTag.contains("-")) {
      String[] parts = eTag.replace("\"", "").split("-");
      String lastPart = parts[parts.length - 1];
      return lastPart.matches("\\d+") ? lastPart : null;
    }
    return null;
  }

  protected int getIOBufferSize(long fileLength) {
    if (bufferSize == 0) {
      // this is mainly for unit tests as init() will not be called in the unit tests
      LOG.warn("buffer size is set to {}", IOUtils.DEFAULT_BUFFER_SIZE);
      bufferSize = IOUtils.DEFAULT_BUFFER_SIZE;
    }
    if (fileLength == 0) {
      // for empty file
      return bufferSize;
    } else {
      return fileLength < bufferSize ? (int) fileLength : bufferSize;
    }
  }

  @Immutable
  protected static final class S3ChunkInputStreamInfo {
    private final MultiDigestInputStream multiDigestInputStream;
    private final long effectiveLength;

    S3ChunkInputStreamInfo(MultiDigestInputStream multiDigestInputStream, long effectiveLength) {
      this.multiDigestInputStream = multiDigestInputStream;
      this.effectiveLength = effectiveLength;
    }

    public MultiDigestInputStream getMultiDigestInputStream() {
      return multiDigestInputStream;
    }

    public long getEffectiveLength() {
      return effectiveLength;
    }
  }
}

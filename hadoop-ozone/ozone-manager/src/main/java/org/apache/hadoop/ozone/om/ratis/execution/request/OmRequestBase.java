package org.apache.hadoop.ozone.om.ratis.execution.request;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OMAuditLogger;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.ratis.execution.DbChangesRecorder;
import org.apache.hadoop.ozone.om.request.RequestAuditor;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.ratis.server.protocol.TermIndex;

/**
 * define methods for request to handle.
 */
public class OmRequestBase implements RequestAuditor {
  private final OMAuditLogger.Builder auditBuilder = OMAuditLogger.newBuilder();
  private DbChangesRecorder recorder = new DbChangesRecorder();
  private OMRequest omRequest;
  private OmBucketInfo bucketInfo;

  public OmRequestBase(OMRequest omRequest, OmBucketInfo bucketInfo) {
    this.omRequest = omRequest;
    this.bucketInfo = bucketInfo;
  }
  /**
   * Perform request validation, bucket type check, update parameter like user Info, update time and others.
   */
  public OzoneManagerProtocolProtos.OMRequest preProcess(OzoneManager ozoneManager)
      throws IOException {
    OzoneManagerProtocolProtos.LayoutVersion layoutVersion = OzoneManagerProtocolProtos.LayoutVersion.newBuilder()
        .setVersion(ozoneManager.getVersionManager().getMetadataLayoutVersion())
        .build();
    omRequest = getOmRequest().toBuilder()
        .setUserInfo(OmKeyUtils.getUserIfNotExists(ozoneManager, getOmRequest()))
        .setLayoutVersion(layoutVersion).build();
    return omRequest;
  }

  public void authorize(OzoneManager ozoneManager) throws IOException {
  }
  /**
   * perform request processing such as prepare changes, resource validation.
   */
  public OMClientResponse process(OzoneManager ozoneManager, TermIndex termIndex) throws IOException {
    return null;
  }
  public DbChangesRecorder changeRecorder() {
    return recorder;
  }
  public OmBucketInfo resolveBucket(OzoneManager ozoneManager, String volume, String bucket) throws IOException {
    String bucketKey = ozoneManager.getMetadataManager().getBucketKey(volume, bucket);

    CacheValue<OmBucketInfo> value = ozoneManager.getMetadataManager().getBucketTable()
        .getCacheValue(new CacheKey<>(bucketKey));
    if (value == null || value.getCacheValue() == null) {
      throw new OMException("Bucket not found: " + volume + "/" + bucket, OMException.ResultCodes.BUCKET_NOT_FOUND);
    }
    bucketInfo = value.getCacheValue();
    return bucketInfo;
  }
  

  protected BucketLayout getBucketLayout() {
    return bucketInfo == null ? BucketLayout.DEFAULT : bucketInfo.getBucketLayout();
  }
  public OmBucketInfo getBucketInfo() {
    return bucketInfo;
  }

  public OMRequest getOmRequest() {
    return omRequest;
  }

  @Override
  public OMAuditLogger.Builder buildAuditMessage(
      AuditAction op, Map< String, String > auditMap, Throwable throwable,
      OzoneManagerProtocolProtos.UserInfo userInfo) {
    auditBuilder.getMessageBuilder()
        .setUser(userInfo != null ? userInfo.getUserName() : null)
        .atIp(userInfo != null ? userInfo.getRemoteAddress() : null)
        .forOperation(op)
        .withParams(auditMap)
        .withResult(throwable != null ? AuditEventStatus.FAILURE :
            AuditEventStatus.SUCCESS)
        .withException(throwable);
    auditBuilder.setAuditMap(auditMap);
    return auditBuilder;
  }

  @Override
  public Map<String, String> buildVolumeAuditMap(String volume) {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, volume);
    return auditMap;
  }
  /**
   * Mark ready for log audit.
   * @param auditLogger
   * @param builder
   */
  protected void markForAudit(AuditLogger auditLogger, OMAuditLogger.Builder builder) {
    builder.setLog(true);
    builder.setAuditLogger(auditLogger);
  }

  public OMAuditLogger.Builder getAuditBuilder() {
    return auditBuilder;
  }
}

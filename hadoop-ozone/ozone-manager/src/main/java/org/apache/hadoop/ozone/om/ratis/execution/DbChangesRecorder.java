package org.apache.hadoop.ozone.om.ratis.execution;

import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Record db changes.
 */
public class DbChangesRecorder {
  private final Map<String, Map<String, CodecBuffer>> tableRecordsMap = new HashMap<>();
  private final Map<Long, BucketChangeInfo> bucketUsedQuotaMap = new HashMap<>();

  public void add(String name, String recordKey, CodecBuffer omKeyCodecBuffer) {
    Map<String, CodecBuffer> recordMap = tableRecordsMap.computeIfAbsent(name, k -> new HashMap<>());
    recordMap.put(recordKey, omKeyCodecBuffer);
  }

  public void add(OmBucketInfo omBucketInfo, long incUsedBytes, long incNamespace) {
    BucketChangeInfo bucketChangeInfo = bucketUsedQuotaMap.computeIfAbsent(omBucketInfo.getObjectID(),
        k -> new BucketChangeInfo(omBucketInfo.getVolumeName(), omBucketInfo.getBucketName()));
    bucketChangeInfo.setIncUsedBytes(incUsedBytes + bucketChangeInfo.getIncUsedBytes());
    bucketChangeInfo.setIncNamespace(incNamespace + bucketChangeInfo.getIncNamespace());
  }

  public Map<String, Map<String, CodecBuffer>> getTableRecordsMap() {
    return tableRecordsMap;
  }

  public Map<Long, BucketChangeInfo> getBucketUsedQuotaMap() {
    return bucketUsedQuotaMap;
  }

  public int getSerializedSize() {
    int tmpSize = 0;
    for (Map.Entry<String, Map<String, CodecBuffer>> tblRecords : getTableRecordsMap().entrySet()) {
      tmpSize += tblRecords.getKey().length();
      for (Map.Entry<String, CodecBuffer> record : tblRecords.getValue().entrySet()) {
        tmpSize += record.getKey().length();
        tmpSize += record.getValue() != null ? record.getValue().readableBytes() : 0;
      }
    }
    return tmpSize;
  }
  public void serialize(OzoneManagerProtocolProtos.PersistDbRequest.Builder reqBuilder) {
    for (Map.Entry<String, Map<String, CodecBuffer>> tblRecords : getTableRecordsMap().entrySet()) {
      OzoneManagerProtocolProtos.DBTableUpdate.Builder tblBuilder
          = OzoneManagerProtocolProtos.DBTableUpdate.newBuilder();
      tblBuilder.setTableName(tblRecords.getKey());
      for (Map.Entry<String, CodecBuffer> record : tblRecords.getValue().entrySet()) {
        OzoneManagerProtocolProtos.DBTableRecord.Builder kvBuild
            = OzoneManagerProtocolProtos.DBTableRecord.newBuilder();
        kvBuild.setKey(ByteString.copyFromUtf8(record.getKey()));
        if (record.getValue() != null) {
          kvBuild.setValue(ByteString.copyFrom(record.getValue().asReadOnlyByteBuffer()));
        }
        tblBuilder.addRecords(kvBuild.build());
      }
      reqBuilder.addTableUpdates(tblBuilder.build());
    }
  }

  public void serializeBucketQuota(Map<Long, OzoneManagerProtocolProtos.BucketQuotaCount.Builder> quotaMap) {
    for (Map.Entry<Long, DbChangesRecorder.BucketChangeInfo> entry : getBucketUsedQuotaMap().entrySet()) {
      OzoneManagerProtocolProtos.BucketQuotaCount.Builder quotaBuilder = quotaMap.computeIfAbsent(
          entry.getKey(), k -> OzoneManagerProtocolProtos.BucketQuotaCount.newBuilder()
              .setVolName(entry.getValue().getVolumeName()).setBucketName(entry.getValue().getBucketName())
              .setBucketObjectId(entry.getKey()).setSupportOldQuota(false));
      quotaBuilder.setDiffUsedBytes(quotaBuilder.getDiffUsedBytes() + entry.getValue().getIncUsedBytes());
      quotaBuilder.setDiffUsedNamespace(quotaBuilder.getDiffUsedNamespace() + entry.getValue().getIncNamespace());
    }
  }
  public void clear() {
    for (Map<String, CodecBuffer> records : tableRecordsMap.values()) {
      records.values().forEach(e -> {
        if (e != null) {
          e.release();
        }
      });
    }
    tableRecordsMap.clear();
    for (Map.Entry<Long, DbChangesRecorder.BucketChangeInfo> entry : getBucketUsedQuotaMap().entrySet()) {
      BucketQuotaResource.BucketQuota bucketQuota = BucketQuotaResource.instance().get(entry.getKey());
      bucketQuota.addUsedBytes(-entry.getValue().getIncUsedBytes());
      bucketQuota.addUsedNamespace(-entry.getValue().getIncNamespace());
    }
    bucketUsedQuotaMap.clear();
  }

  /**
   * bucket change info.
   */
  public static class BucketChangeInfo {
    private final String volumeName;
    private final String bucketName;
    private long incUsedBytes;
    private long incNamespace;
    public BucketChangeInfo(String volumeName, String bucketName) {
      this.volumeName = volumeName;
      this.bucketName = bucketName;
    }

    public String getVolumeName() {
      return volumeName;
    }

    public String getBucketName() {
      return bucketName;
    }

    public long getIncUsedBytes() {
      return incUsedBytes;
    }

    public void setIncUsedBytes(long incUsedBytes) {
      this.incUsedBytes = incUsedBytes;
    }

    public long getIncNamespace() {
      return incNamespace;
    }

    public void setIncNamespace(long incNamespace) {
      this.incNamespace = incNamespace;
    }
  }
}

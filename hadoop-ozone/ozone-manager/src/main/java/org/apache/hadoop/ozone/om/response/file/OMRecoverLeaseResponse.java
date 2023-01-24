package org.apache.hadoop.ozone.om.response.file;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.key.OmKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
        .OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_FILE_TABLE;

/**
 * Performs tasks for RecoverLease request responses.
 */
@CleanupTableInfo(cleanupTables = {FILE_TABLE, OPEN_FILE_TABLE})
public class OMRecoverLeaseResponse extends OmKeyResponse {

  private String openKeyName;
  public OMRecoverLeaseResponse(@Nonnull OMResponse omResponse,
      BucketLayout bucketLayout, String openKeyName) {
    super(omResponse, bucketLayout);
    this.openKeyName = openKeyName;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMRecoverLeaseResponse(@Nonnull OMResponse omResponse,
      @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    checkStatusNotOK();
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    // Delete from OpenKey table
    if (openKeyName != null) {
      omMetadataManager.getOpenKeyTable(getBucketLayout()).deleteWithBatch(
          batchOperation, openKeyName);
    }
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}

package org.apache.hadoop.ozone.om.request.bucket;

import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.newBucketInfoBuilder;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.newCreateBucketRequest;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestOMBucketCreateRequestWithObjectStore extends TestOMBucketCreateRequest {

  @BeforeEach
  public void setupWithObjectStore() {
    ozoneManager.getConfiguration().set(
        OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        OMConfigKeys.OZONE_BUCKET_LAYOUT_OBJECT_STORE); // 或 LEGACY
  }

  @Test
  public void testNonS3BucketNameRejectedForObjectStoreWhenStrictDisabled()
      throws Exception {

    // strict mode disabled
    ozoneManager.getConfiguration().setBoolean(
        OMConfigKeys.OZONE_OM_NAMESPACE_STRICT_S3, false);

    String volumeName = UUID.randomUUID().toString();
    String bucketName = "bucket_with_underscore"; // non-S3-compliant
    addCreateVolumeToTable(volumeName, omMetadataManager);

    // Explicitly set bucket layout to OBJECT_STORE so the test doesn't depend on
    // defaults or mocked OM behavior.
    OzoneManagerProtocolProtos.BucketInfo.Builder bucketInfo =
        newBucketInfoBuilder(bucketName, volumeName)
            .setBucketLayout(
                OzoneManagerProtocolProtos.BucketLayoutProto.OBJECT_STORE);

    OMRequest originalRequest = newCreateBucketRequest(bucketInfo).build();
    OMBucketCreateRequest req = new OMBucketCreateRequest(originalRequest);

    OMException ex = assertThrows(OMException.class,
        () -> req.preExecute(ozoneManager));

    assertEquals(OMException.ResultCodes.INVALID_BUCKET_NAME, ex.getResult());
  }
}

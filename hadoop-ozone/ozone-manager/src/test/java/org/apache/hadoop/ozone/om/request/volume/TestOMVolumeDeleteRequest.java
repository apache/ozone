/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.volume;

import java.util.UUID;

import com.google.common.base.Optional;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests delete volume request.
 */
public class TestOMVolumeDeleteRequest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;
  private AuditLogger auditLogger;


  @Before
  public void setup() throws Exception {
    ozoneManager = mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    auditLogger = Mockito.mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    Mockito.doNothing().when(auditLogger).logWrite(any(AuditMessage.class));

  }

  @After
  public void stop() {
    omMetrics.unRegister();
    Mockito.framework().clearInlineMocks();
  }

  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String ownerName = UUID.randomUUID().toString();
    OMRequest originalRequest = deleteVolumeRequest(volumeName, ownerName);

    OMVolumeDeleteRequest omVolumeDeleteRequest =
        new OMVolumeDeleteRequest(originalRequest);

    OMRequest modifiedRequest = omVolumeDeleteRequest.preExecute(ozoneManager);
    Assert.assertNotEquals(originalRequest, modifiedRequest);
  }


  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String ownerName = "user1";

    OMRequest originalRequest = deleteVolumeRequest(volumeName, ownerName);

    OMVolumeDeleteRequest omVolumeDeleteRequest =
        new OMVolumeDeleteRequest(originalRequest);

    omVolumeDeleteRequest.preExecute(ozoneManager);

    // Add volume and user to DB
    TestOMRequestUtils.addVolumeToDB(volumeName, ownerName, omMetadataManager);
    TestOMRequestUtils.addUserToDB(volumeName, ownerName, omMetadataManager);

    String volumeKey = omMetadataManager.getVolumeKey(volumeName);
    String ownerKey = omMetadataManager.getUserKey(ownerName);


    Assert.assertNotNull(omMetadataManager.getVolumeTable().get(volumeKey));
    Assert.assertNotNull(omMetadataManager.getUserTable().get(ownerKey));

    OMClientResponse omClientResponse =
        omVolumeDeleteRequest.validateAndUpdateCache(ozoneManager, 1);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateVolumeResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());



    Assert.assertTrue(omMetadataManager.getUserTable().get(ownerKey)
        .getVolumeNamesList().size() == 0);
    // As now volume is deleted, table should not have those entries.
    Assert.assertNull(omMetadataManager.getVolumeTable().get(volumeKey));

  }


  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String ownerName = "user1";

    OMRequest originalRequest = deleteVolumeRequest(volumeName, ownerName);

    OMVolumeDeleteRequest omVolumeDeleteRequest =
        new OMVolumeDeleteRequest(originalRequest);

    omVolumeDeleteRequest.preExecute(ozoneManager);

    OMClientResponse omClientResponse =
        omVolumeDeleteRequest.validateAndUpdateCache(ozoneManager, 1);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateVolumeResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omResponse.getStatus());

  }


  @Test
  public void testValidateAndUpdateCacheWithVolumeNotEmpty() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String ownerName = "user1";

    OMRequest originalRequest = deleteVolumeRequest(volumeName, ownerName);

    OMVolumeDeleteRequest omVolumeDeleteRequest =
        new OMVolumeDeleteRequest(originalRequest);

    omVolumeDeleteRequest.preExecute(ozoneManager);

    // Add some bucket to bucket table cache.
    String bucketName = UUID.randomUUID().toString();
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);

    OmBucketInfo omBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName).setBucketName(bucketName).build();
    omMetadataManager.getBucketTable().addCacheEntry(new CacheKey<>(bucketKey),
        new CacheValue<>(Optional.of(omBucketInfo), 1L));

    // Add user and volume to DB.
    TestOMRequestUtils.addUserToDB(volumeName, ownerName, omMetadataManager);
    TestOMRequestUtils.addVolumeToDB(volumeName, ownerName, omMetadataManager);

    OMClientResponse omClientResponse =
        omVolumeDeleteRequest.validateAndUpdateCache(ozoneManager, 1L);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateVolumeResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_EMPTY,
        omResponse.getStatus());
  }

  /**
   * Create OMRequest for delete volume.
   * @param volumeName
   * @param ownerName
   * @return OMRequest
   */
  private OMRequest deleteVolumeRequest(String volumeName,
      String ownerName) {
    DeleteVolumeRequest deleteVolumeRequest =
        DeleteVolumeRequest.newBuilder().setVolumeName(volumeName)
            .setOwner(ownerName).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteVolume)
        .setDeleteVolumeRequest(deleteVolumeRequest).build();
  }
}

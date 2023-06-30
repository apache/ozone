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
package org.apache.hadoop.ozone.om.ratis;

import com.google.protobuf.ProtocolMessageEnum;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.INVALID_REQUEST;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

/**
 * Test OM Ratis request handling.
 */
public class TestOzoneManagerRatisRequest {
  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private OzoneManager ozoneManager;
  private OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
  private OMMetadataManager omMetadataManager;

  @Test(timeout = 300_000)
  public void testRequestWithNonExistentBucket()
      throws Exception {
    // Test: Creating a client request for a bucket which doesn't exist.
    ozoneManager = Mockito.mock(OzoneManager.class);
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration,
        ozoneManager);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);

    String volumeName = "vol1";
    String bucketName = "invalidBuck";

    // Add entry to Volume Table.
    OmVolumeArgs omVolumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setOwnerName("owner")
        .setAdminName("admin")
        .build();
    omMetadataManager.getVolumeTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getVolumeKey(volumeName)),
        CacheValue.get(100L, omVolumeArgs));

    OzoneManagerProtocolProtos.OMRequest omRequest = OMRequestTestUtils
        .createCompleteMPURequest(volumeName, bucketName, "mpuKey", "mpuKeyID",
            new ArrayList<>());

    try {
      // Request creation flow should throw exception if the bucket associated
      // with the request doesn't exist.
      OzoneManagerRatisUtils.createClientRequest(omRequest, ozoneManager);
      fail("Expected OMException: Bucket not found");
    } catch (OMException oe) {
      // Expected exception.
      Assert.assertEquals(OMException.ResultCodes.BUCKET_NOT_FOUND,
          oe.getResult());
    }
  }

  @Test
  public void testUnknownRequestHandling() throws IOException,
      ServiceException {
    // Create an instance of OMRequest with an unknown command type.
    OzoneManagerProtocolProtos.OMRequest omRequest =
        OzoneManagerProtocolProtos.OMRequest.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.TestUnknownCommand)
            .setClientId("test-client-id")
            .build();

    ozoneManager = Mockito.mock(OzoneManager.class);
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration,
        ozoneManager);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);

    OzoneManagerRatisServer ratisServer =
        Mockito.mock(OzoneManagerRatisServer.class);
    ProtocolMessageMetrics<ProtocolMessageEnum> protocolMessageMetrics =
        Mockito.mock(ProtocolMessageMetrics.class);
    long lastTransactionIndexForNonRatis = 100L;

    OzoneManagerProtocolProtos.OMResponse expectedResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder()
            .setStatus(INVALID_REQUEST)
            .setCmdType(omRequest.getCmdType())
            .setTraceID(omRequest.getTraceID())
            .setSuccess(false)
            .setMessage("Unrecognized write command type request " +
                omRequest.getCmdType())
            .build();

    boolean[] enableRatisValues = {true, false};
    for (boolean enableRatis : enableRatisValues) {
      OzoneManagerProtocolServerSideTranslatorPB serverSideTranslatorPB =
          new OzoneManagerProtocolServerSideTranslatorPB(ozoneManager,
              ratisServer, protocolMessageMetrics, enableRatis,
              lastTransactionIndexForNonRatis);

      OzoneManagerProtocolProtos.OMResponse actualResponse =
          serverSideTranslatorPB.processRequest(omRequest);

      Assertions.assertEquals(expectedResponse, actualResponse);
    }
  }
}

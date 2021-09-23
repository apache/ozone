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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadCompleteRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.util.ArrayList;

import static org.mockito.Mockito.when;

/**
 * Test: Creating a client request for a bucket which doesn't exist.
 */
public class TestOzoneManagerRatisRequest {
  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private OzoneManager ozoneManager;
  private OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
  private OMMetadataManager omMetadataManager;

  @Test(timeout = 300_000)
  public void testRequestWithNonExistentBucket()
      throws Exception {
    ozoneManager = Mockito.mock(OzoneManager.class);
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);

    String volumeName = "vol1";
    String bucketName = "invalidBuck";
    OzoneManagerProtocolProtos.OMRequest omRequest = TestOMRequestUtils
        .createCompleteMPURequest(volumeName, bucketName, "mpuKey", "mpuKeyID",
            new ArrayList<>());

    OMClientRequest req =
        OzoneManagerRatisUtils.createClientRequest(omRequest, ozoneManager);
    Assert.assertNotNull(req);
    Assert.assertTrue("Unexpected request on invalid bucket",
        req instanceof S3MultipartUploadCompleteRequest);
  }
}

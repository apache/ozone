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

package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.KeyInsightInfoResp;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TemporaryFolder;

import javax.ws.rs.core.Response;
import java.util.Random;

import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getBucketLayout;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getMockOzoneManagerServiceProvider;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;

/**
 * Unit test for OmDBInsightEndPoint.
 */
public class TestOmDBInsightEndPoint {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private OMMetadataManager omMetadataManager;
  private ReconOMMetadataManager reconOMMetadataManager;
  private OzoneManagerServiceProviderImpl ozoneManagerServiceProvider;
  private OMDBInsightEndpoint omdbInsightEndpoint;
  private Random random = new Random();

  @Before
  public void setUp() throws Exception {
    omMetadataManager = initializeNewOmMetadataManager(
        temporaryFolder.newFolder());
    ozoneManagerServiceProvider = getMockOzoneManagerServiceProvider();
    reconOMMetadataManager = getTestReconOmMetadataManager(omMetadataManager,
        temporaryFolder.newFolder());
    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder)
            .withReconSqlDb()
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(ozoneManagerServiceProvider)
            .addBinding(OMDBInsightEndpoint.class)
            .build();
    omdbInsightEndpoint = reconTestInjector.getInstance(
        OMDBInsightEndpoint.class);
  }

  @Test
  public void testGetOpenKeyInfo() throws Exception {
    OmKeyInfo omKeyInfo = getOmKeyInfo("sampleVol", "bucketOne", "key_one");

    reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_one", omKeyInfo);
    OmKeyInfo omKeyInfo1 =
        reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
            .get("/sampleVol/bucketOne/key_one");
    Assertions.assertEquals("key_one", omKeyInfo1.getKeyName());
    Response openKeyInfoResp = omdbInsightEndpoint.getOpenKeyInfo(-1, "");
    KeyInsightInfoResp keyInsightInfoResp =
        (KeyInsightInfoResp) openKeyInfoResp.getEntity();
    Assertions.assertNotNull(keyInsightInfoResp);
    Assertions.assertEquals("key_one",
        keyInsightInfoResp.getNonFSOKeyInfoList().get(0).getPath());
  }

  @Test
  public void testGetDeletedKeyInfo() throws Exception {
    OmKeyInfo omKeyInfo = getOmKeyInfo("sampleVol", "bucketOne", "key_one");

    reconOMMetadataManager.getKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_one", omKeyInfo);
    OmKeyInfo omKeyInfo1 = reconOMMetadataManager.getKeyTable(getBucketLayout())
        .get("/sampleVol/bucketOne/key_one");
    Assertions.assertEquals("key_one", omKeyInfo1.getKeyName());
    RepeatedOmKeyInfo repeatedOmKeyInfo = new RepeatedOmKeyInfo(omKeyInfo);
    reconOMMetadataManager.getDeletedTable()
        .put("/sampleVol/bucketOne/key_one", repeatedOmKeyInfo);
    RepeatedOmKeyInfo repeatedOmKeyInfo1 =
        reconOMMetadataManager.getDeletedTable()
            .get("/sampleVol/bucketOne/key_one");
    Assertions.assertEquals("key_one",
        repeatedOmKeyInfo1.getOmKeyInfoList().get(0).getKeyName());
    Response deletedKeyInfo = omdbInsightEndpoint.getDeletedKeyInfo(-1, "");
    KeyInsightInfoResp keyInsightInfoResp =
        (KeyInsightInfoResp) deletedKeyInfo.getEntity();
    Assertions.assertNotNull(keyInsightInfoResp);
    Assertions.assertEquals("key_one",
        keyInsightInfoResp.getRepeatedOmKeyInfoList().get(0).getOmKeyInfoList()
            .get(0).getKeyName());
  }

  private OmKeyInfo getOmKeyInfo(String volumeName, String bucketName,
                                 String keyName) {
    return new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setReplicationConfig(StandaloneReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.ONE))
        .setDataSize(random.nextLong())
        .build();
  }

}

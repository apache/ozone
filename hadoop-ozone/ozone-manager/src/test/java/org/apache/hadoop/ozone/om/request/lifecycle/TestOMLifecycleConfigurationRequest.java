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

package org.apache.hadoop.ozone.om.request.lifecycle;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketLayoutProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteLifecycleConfigurationRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleAction;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleConfiguration;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleExpiration;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleFilter;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleRule;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetLifecycleConfigurationRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Base test class for Lifecycle configuration request.
 */
@SuppressWarnings("visibilitymodifier")
public class TestOMLifecycleConfigurationRequest {

  @TempDir
  private File tempDir;

  protected OzoneManager ozoneManager;
  protected OMMetrics omMetrics;
  protected OMMetadataManager omMetadataManager;
  protected AuditLogger auditLogger;

  @BeforeEach
  public void setup() throws Exception {
    ozoneManager = mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS, tempDir.getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration, ozoneManager);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getMaxUserVolumeCount()).thenReturn(10L);
    when(ozoneManager.resolveBucketLink(any(Pair.class), any(OMClientRequest.class)))
        .thenAnswer(i -> new ResolvedBucket(i.getArgument(0),
            i.getArgument(0), "dummyBucketOwner", BucketLayout.OBJECT_STORE));
    OMLayoutVersionManager lvm = mock(OMLayoutVersionManager.class);
    when(lvm.getMetadataLayoutVersion()).thenReturn(0);
    when(ozoneManager.getVersionManager()).thenReturn(lvm);
    auditLogger = mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    Mockito.doNothing().when(auditLogger).logWrite(any(AuditMessage.class));
  }

  @AfterEach
  public void stop() {
    omMetrics.unRegister();
    Mockito.framework().clearInlineMocks();
  }

  public OMRequest createDeleteLifecycleConfigurationRequest(
      String volumeName, String bucketName) {
    return OMRequest.newBuilder().setDeleteLifecycleConfigurationRequest(
            DeleteLifecycleConfigurationRequest.newBuilder()
                .setVolumeName(volumeName)
                .setBucketName(bucketName))
        .setCmdType(Type.DeleteLifecycleConfiguration)
        .setClientId(UUID.randomUUID().toString()).build();
  }

  public static void addVolumeAndBucketToTable(String volumeName,
      String bucketName, String ownerName, OMMetadataManager omMetadataManager)
      throws Exception {
    OMRequestTestUtils.addVolumeToDB(volumeName, ownerName, omMetadataManager);
    OMRequestTestUtils.addBucketToDB(volumeName, bucketName, omMetadataManager);
  }

  public OMRequest setLifecycleConfigurationRequest(String volumeName,
      String bucketName, String ownerName) {
    return setLifecycleConfigurationRequest(volumeName, bucketName,
        ownerName, true);
  }

  public OMRequest setLifecycleConfigurationRequest(String volumeName,
      String bucketName, String ownerName, boolean addRules) {
    String prefix = "prefix/";
    LifecycleConfiguration.Builder builder = LifecycleConfiguration.newBuilder()
        .setBucketLayout(BucketLayoutProto.OBJECT_STORE)
        .setCreationTime(System.currentTimeMillis())
        .setVolume(volumeName)
        .setBucket(bucketName);

    if (addRules) {
      builder.addRules(LifecycleRule.newBuilder()
          .setId(RandomStringUtils.randomAlphabetic(32))
          .setEnabled(true)
          .addAction(LifecycleAction.newBuilder()
              .setExpiration(LifecycleExpiration.newBuilder().setDays(3).build()))
          .setFilter(LifecycleFilter.newBuilder().setPrefix(prefix))
      );
    }

    LifecycleConfiguration lcc = builder.build();

    SetLifecycleConfigurationRequest setLifecycleConfigurationRequest =
        SetLifecycleConfigurationRequest.newBuilder()
            .setLifecycleConfiguration(lcc)
            .build();

    return OMRequest.newBuilder().setSetLifecycleConfigurationRequest(
            setLifecycleConfigurationRequest)
        .setCmdType(Type.SetLifecycleConfiguration)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }
}

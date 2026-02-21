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

package org.apache.hadoop.ozone.om.request.volume;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.framework;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

/**
 * Base test class for Volume request.
 */
@SuppressWarnings("visibilitymodifier")
public class TestOMVolumeRequest {

  @TempDir
  private Path folder;

  protected OzoneManager ozoneManager;
  protected OMMetrics omMetrics;
  protected OMMetadataManager omMetadataManager;
  protected AuditLogger auditLogger;

  @BeforeEach
  public void setup() throws Exception {
    ozoneManager = mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.toAbsolutePath().toString());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration,
        ozoneManager);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(ozoneManager.getMaxUserVolumeCount()).thenReturn(10L);
    OMLayoutVersionManager lvm = mock(OMLayoutVersionManager.class);
    when(lvm.getMetadataLayoutVersion()).thenReturn(0);
    when(ozoneManager.getVersionManager()).thenReturn(lvm);
    auditLogger = mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    doNothing().when(auditLogger).logWrite(any(AuditMessage.class));
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
    when(ozoneManager.getConfig()).thenReturn(ozoneConfiguration.getObject(OmConfig.class));
  }

  @AfterEach
  public void stop() {
    omMetrics.unRegister();
    framework().clearInlineMocks();
  }

  /**
   * Create OMRequest for create volume.
   * @param volumeName
   * @param adminName
   * @param ownerName
   * @param acl
   * @return OMRequest
   */
  static OMRequest createVolumeRequest(String volumeName, String adminName,
      String ownerName, String acl) {
    VolumeInfo volumeInfo = VolumeInfo.newBuilder()
        .setVolume(volumeName)
        .setAdminName(adminName)
        .setOwnerName(ownerName)
        .addVolumeAcls(OzoneAcl.toProtobuf(OzoneAcl.parseAcl(acl)))
        .build();
    CreateVolumeRequest createVolumeRequest =
        CreateVolumeRequest.newBuilder().setVolumeInfo(volumeInfo).build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.CreateVolume)
        .setCreateVolumeRequest(createVolumeRequest).build();
  }

  static OMRequest createDeleteVolumeRequest(String volumeName) {
    OzoneManagerProtocolProtos.DeleteVolumeRequest deleteVolumeRequest =
        OzoneManagerProtocolProtos.DeleteVolumeRequest.newBuilder()
            .setVolumeName(volumeName)
            .build();

    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteVolume)
        .setDeleteVolumeRequest(deleteVolumeRequest)
        .build();
  }

}

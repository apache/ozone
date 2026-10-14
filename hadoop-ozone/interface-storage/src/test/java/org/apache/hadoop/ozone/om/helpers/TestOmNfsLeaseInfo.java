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

package org.apache.hadoop.ozone.om.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedNfsLeaseInfo;
import org.junit.jupiter.api.Test;

class TestOmNfsLeaseInfo {
  @Test
  void codecRoundTripsExportLease() throws Exception {
    OmNfsLeaseInfo lease = OmNfsLeaseInfo.getFromProtobuf(PersistedNfsLeaseInfo.newBuilder()
        .setKind(PersistedNfsLeaseInfo.Kind.EXPORT)
        .setExportUUID("a7ad8fd1-c88d-4f1b-bf58-38406e931adc")
        .setGeneration(5)
        .setExpiresAtMillis(1234)
        .setGatewayID("gateway-1")
        .setGatewayGeneration(3)
        .setVolumeID(-512)
        .setBucketID(-256)
        .build());

    assertEquals(lease, OmNfsLeaseInfo.getCodec().fromPersistedFormat(
        OmNfsLeaseInfo.getCodec().toPersistedFormat(lease)));
  }

  @Test
  void codecRoundTripsFileLease() throws Exception {
    OmNfsLeaseInfo lease = OmNfsLeaseInfo.getFromProtobuf(PersistedNfsLeaseInfo.newBuilder()
        .setKind(PersistedNfsLeaseInfo.Kind.FILE)
        .setExportUUID("a7ad8fd1-c88d-4f1b-bf58-38406e931adc")
        .setGeneration(8)
        .setExpiresAtMillis(4321)
        .setGatewayID("gateway-1")
        .setGatewayGeneration(3)
        .setVolumeID(-512)
        .setBucketID(-256)
        .setObjectID(-1)
        .setTransferID("transfer-1")
        .setOwnerToken(ByteString.copyFromUtf8("opaque-owner-token"))
        .setOwnerPrincipal("nfs-user@EXAMPLE.COM")
        .build());

    assertEquals(lease, OmNfsLeaseInfo.getCodec().fromPersistedFormat(
        OmNfsLeaseInfo.getCodec().toPersistedFormat(lease)));
  }

  @Test
  void rejectsLeaseMissingKindSpecificFields() {
    PersistedNfsLeaseInfo export = PersistedNfsLeaseInfo.newBuilder()
        .setKind(PersistedNfsLeaseInfo.Kind.EXPORT)
        .setExportUUID("export")
        .setGeneration(1)
        .setExpiresAtMillis(1)
        .setVolumeID(1)
        .setBucketID(2)
        .build();
    PersistedNfsLeaseInfo file = PersistedNfsLeaseInfo.newBuilder()
        .setKind(PersistedNfsLeaseInfo.Kind.FILE)
        .setExportUUID("export")
        .setGeneration(1)
        .setExpiresAtMillis(1)
        .setGatewayID("gateway")
        .setGatewayGeneration(1)
        .build();

    assertThrows(IllegalArgumentException.class, () -> OmNfsLeaseInfo.getFromProtobuf(export));
    assertThrows(IllegalArgumentException.class, () -> OmNfsLeaseInfo.getFromProtobuf(file));
  }

  @Test
  void acquireFencesExpiredExportLease() {
    OmNfsLeaseInfo first = OmNfsLeaseInfo.acquireExportLease(null, "export-1", 10, 20, "gateway-a", 100, 50);
    OmNfsLeaseInfo second = OmNfsLeaseInfo.acquireExportLease(first, "export-1", 10, 20, "gateway-b", 150, 50);

    assertEquals(2, second.getProtobuf().getGeneration());
    assertEquals(2, second.getProtobuf().getGatewayGeneration());
    assertEquals("gateway-b", second.getProtobuf().getGatewayID());
  }

  @Test
  void acquireRejectsActiveLeaseAndBucketRebinding() {
    OmNfsLeaseInfo lease = OmNfsLeaseInfo.acquireExportLease(null, "export-1", 10, 20, "gateway-a", 100, 50);

    assertThrows(IllegalStateException.class,
        () -> OmNfsLeaseInfo.acquireExportLease(lease, "export-1", 10, 20, "gateway-b", 149, 50));
    assertThrows(IllegalArgumentException.class,
        () -> OmNfsLeaseInfo.acquireExportLease(lease, "export-1", 10, 21, "gateway-b", 150, 50));
  }

  @Test
  void renewAndReleaseRequireCurrentOwner() {
    OmNfsLeaseInfo lease = OmNfsLeaseInfo.acquireExportLease(null, "export-1", 10, 20, "gateway-a", 100, 50);
    OmNfsLeaseInfo renewed = lease.renewExportLease("gateway-a", 1, 1, 120, 50);
    assertEquals(170, renewed.getProtobuf().getExpiresAtMillis());
    assertThrows(IllegalStateException.class, () -> renewed.renewExportLease("gateway-a", 1, 2, 121, 50));

    OmNfsLeaseInfo released = renewed.releaseExportLease("gateway-a", 1, 1, 130);
    assertEquals(2, released.getProtobuf().getGeneration());
    assertEquals(130, released.getProtobuf().getExpiresAtMillis());
    assertThrows(IllegalStateException.class, () -> released.renewExportLease("gateway-a", 1, 1, 130, 50));
  }

  @Test
  void fileLeaseRequiresCurrentExportAndOwnerFences() {
    OmNfsLeaseInfo export = OmNfsLeaseInfo.acquireExportLease(null, "export-1", 10, 20, "gateway-a", 100, 1000);
    ByteString token = ByteString.copyFromUtf8("owner-token");
    OmNfsLeaseInfo file = OmNfsLeaseInfo.acquireFileLease(null, export, 30, "transfer-1",
        "nfs-user@EXAMPLE.COM", token, 100, 500);
    assertEquals(1, file.getProtobuf().getGeneration());
    assertEquals(600, file.getProtobuf().getExpiresAtMillis());
    assertEquals("nfs-user@EXAMPLE.COM", file.getOwnerPrincipal());
    file.validateFileCommit(export, "export-1", "gateway-a", 1, "transfer-1", 1, token, 150);
    assertThrows(IllegalStateException.class,
        () -> file.validateFileCommit(export, "export-1", "gateway-a", 1, "transfer-1", 1,
            ByteString.copyFromUtf8("stale-token"), 150));
    assertThrows(IllegalStateException.class,
        () -> file.validateFileCommit(export, "export-1", "gateway-a", 1, "transfer-1", 1, token, 600));

    OmNfsLeaseInfo renewedExport = export.renewExportLease("gateway-a", 1, 1, 200, 1000);
    OmNfsLeaseInfo publishedFile = file.withContentPublished(true);
    OmNfsLeaseInfo renewed = publishedFile.renewFileLease(renewedExport, 1, "transfer-1", token, 200, 500);
    assertEquals(700, renewed.getProtobuf().getExpiresAtMillis());
    assertEquals("nfs-user@EXAMPLE.COM", renewed.getOwnerPrincipal());
    assertTrue(renewed.getProtobuf().getContentPublished());
    assertThrows(IllegalStateException.class,
        () -> renewed.renewFileLease(renewedExport, 2, "transfer-1", token, 201, 500));
    OmNfsLeaseInfo replacement = OmNfsLeaseInfo.acquireExportLease(renewedExport, "export-1", 10, 20,
        "gateway-b", 1200, 1000);
    assertThrows(IllegalStateException.class,
        () -> renewed.renewFileLease(replacement, 1, "transfer-1", token, 1201, 500));

    OmNfsLeaseInfo released = renewed.releaseFileLease(renewedExport, 1, "transfer-1", token, 250);
    assertEquals(2, released.getProtobuf().getGeneration());
    assertEquals(250, released.getProtobuf().getExpiresAtMillis());
    assertThrows(IllegalStateException.class,
        () -> released.renewFileLease(renewedExport, 1, "transfer-1", token, 251, 500));
  }

  @Test
  void recoveryRotatesOwnerTokenForTheSamePrincipalAfterGatewayTakeover() {
    String exportUuid = "export-1";
    String principal = "nfs-user@EXAMPLE.COM";
    ByteString originalToken = ByteString.copyFromUtf8("original-owner-token");
    OmNfsLeaseInfo oldExport = OmNfsLeaseInfo.acquireExportLease(
        null, exportUuid, 10, 20, "gateway-a", 100, 100);
    OmNfsLeaseInfo file = OmNfsLeaseInfo.acquireFileLease(null, oldExport, 30, "transfer-1",
        principal, originalToken, 100, 80);
    OmNfsLeaseInfo newExport = OmNfsLeaseInfo.acquireExportLease(
        oldExport, exportUuid, 10, 20, "gateway-b", 200, 1000);
    ByteString replacementToken = ByteString.copyFromUtf8("replacement-owner-token");

    OmNfsLeaseInfo recovered = file.recoverFileLease(newExport, principal, replacementToken, 201, 500);

    assertEquals(2, recovered.getProtobuf().getGeneration());
    assertEquals(2, recovered.getProtobuf().getGatewayGeneration());
    assertEquals("gateway-b", recovered.getProtobuf().getGatewayID());
    assertEquals("transfer-1", recovered.getProtobuf().getTransferID());
    assertEquals(principal, recovered.getOwnerPrincipal());
    assertEquals(replacementToken, recovered.getProtobuf().getOwnerToken());
    assertFalse(recovered.getProtobuf().getContentPublished());
    assertThrows(IllegalStateException.class,
        () -> file.recoverFileLease(newExport, "other-user@EXAMPLE.COM", replacementToken, 201, 500));
  }

  @Test
  void recoveryPreservesPublishedContentAndNewStagingCanClearIt() {
    OmNfsLeaseInfo export = OmNfsLeaseInfo.acquireExportLease(null, "export-1", 10, 20, "gateway-a", 100, 1000);
    ByteString token = ByteString.copyFromUtf8("owner-token");
    OmNfsLeaseInfo file = OmNfsLeaseInfo.acquireFileLease(null, export, 30, "transfer-1",
        "nfs-user@EXAMPLE.COM", token, 100, 500).withContentPublished(true);

    OmNfsLeaseInfo recovered = file.recoverFileLease(export, "nfs-user@EXAMPLE.COM",
        ByteString.copyFromUtf8("replacement-token"), 200, 500);

    assertTrue(recovered.getProtobuf().getContentPublished());
    assertFalse(recovered.withContentPublished(false).getProtobuf().getContentPublished());
  }
}

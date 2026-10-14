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

package org.apache.hadoop.ozone.nfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests export isolation independently of OM existence and ACL checks. */
class TestNfsExportBinding {
  private static final UUID EXPORT_ID = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
  private final NfsExportBinding binding = new NfsExportBinding(EXPORT_ID, "cluster-a", "volume", "bucket",
      Long.MIN_VALUE, -1);

  private OzoneConfiguration configuration() {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set("ozone.nfs.export.data.uuid", EXPORT_ID.toString());
    configuration.set("ozone.nfs.export.data.cluster.id", "cluster-a");
    configuration.set("ozone.nfs.export.data.volume", "volume");
    configuration.set("ozone.nfs.export.data.bucket", "bucket");
    configuration.set("ozone.nfs.export.data.volume.id", Long.toUnsignedString(Long.MIN_VALUE));
    configuration.set("ozone.nfs.export.data.bucket.id", Long.toUnsignedString(-1));
    return configuration;
  }

  @Test
  void loadsSameBindingAcrossReplacement() throws Exception {
    NfsExportBinding loaded = NfsExportBinding.fromConfiguration(configuration(), "data");
    NfsExportBinding replacement = NfsExportBinding.fromConfiguration(configuration(), "data");
    assertEquals(binding.getRootHandle(), loaded.getRootHandle());
    assertEquals(loaded.getRootHandle(), replacement.getRootHandle());
    assertEquals(binding.getClusterId(), loaded.getClusterId());
    assertEquals(binding.getVolumeName(), loaded.getVolumeName());
    assertEquals(binding.getBucketName(), loaded.getBucketName());
    loaded.validateHandle("cluster-a", replacement.getRootHandle());
  }

  @ParameterizedTest
  @ValueSource(strings = {"uuid", "cluster.id", "volume", "bucket", "volume.id", "bucket.id"})
  void refusesIncompleteConfiguration(String property) {
    OzoneConfiguration configuration = configuration();
    String key = "ozone.nfs.export.data." + property;
    configuration.unset(key);
    assertThrows(IllegalArgumentException.class, () -> NfsExportBinding.fromConfiguration(configuration, "data"));
    configuration.set(key, " ");
    assertThrows(IllegalArgumentException.class, () -> NfsExportBinding.fromConfiguration(configuration, "data"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"1-1-1-1-1", "invalid", "00112233445566778899aabbccddeeff"})
  void refusesMalformedExportUuid(String uuid) {
    OzoneConfiguration configuration = configuration();
    configuration.set("ozone.nfs.export.data.uuid", uuid);
    assertThrows(IllegalArgumentException.class, () -> NfsExportBinding.fromConfiguration(configuration, "data"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"-1", "18446744073709551616", "not-an-id"})
  void refusesInvalidUnsignedIds(String id) {
    OzoneConfiguration configuration = configuration();
    for (String property : new String[] {"volume.id", "bucket.id"}) {
      configuration.set("ozone.nfs.export.data." + property, id);
      OzoneConfiguration invalid = configuration;
      assertThrows(IllegalArgumentException.class, () -> NfsExportBinding.fromConfiguration(invalid, "data"));
      configuration = configuration();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "data.uuid", "../data", "data/other", " data", "data "})
  void refusesAmbiguousConfigurationNames(String name) {
    assertThrows(IllegalArgumentException.class, () -> NfsExportBinding.fromConfiguration(configuration(), name));
  }

  @ParameterizedTest
  @EnumSource(NfsFileHandle.Kind.class)
  void acceptsMatchingBinding(NfsFileHandle.Kind kind) throws Exception {
    NfsFileHandle handle = new NfsFileHandle(EXPORT_ID, Long.MIN_VALUE, -1,
        kind == NfsFileHandle.Kind.BUCKET ? -1 : Long.MAX_VALUE, kind);
    binding.validateHandle("cluster-a", NfsFileHandle.fromBytes(handle.toBytes()));
    assertEquals(EXPORT_ID, binding.getExportId());
    assertEquals("cluster-a", binding.getClusterId());
    assertEquals("volume", binding.getVolumeName());
    assertEquals("bucket", binding.getBucketName());
    assertEquals(Long.MIN_VALUE, binding.getVolumeId());
    assertEquals(-1, binding.getBucketId());
  }

  @ParameterizedTest
  @ValueSource(strings = {"cluster", "unknown-cluster", "export", "volume", "bucket", "root-id", "file-root"})
  void rejectsCrossExportAndIncarnation(String mismatch) {
    NfsFileHandle handle = new NfsFileHandle(mismatch.equals("export") ? UUID.randomUUID() : EXPORT_ID,
        mismatch.equals("volume") ? 1 : Long.MIN_VALUE, mismatch.equals("bucket") ? 2 : -1,
        mismatch.equals("file-root") ? -1 : Long.MAX_VALUE,
        mismatch.equals("root-id") ? NfsFileHandle.Kind.BUCKET : NfsFileHandle.Kind.FILE);
    String connectedClusterId = mismatch.equals("cluster") ? "cluster-b"
        : mismatch.equals("unknown-cluster") ? null : "cluster-a";
    OMException exception = assertThrows(OMException.class, () -> binding.validateHandle(connectedClusterId, handle));
    assertEquals(NfsStatus.STALE, NfsStatus.fromOmException(exception, NfsStatus.LookupContext.HANDLE));
  }

  @Test
  void replacementRetainsRootIdentity() throws Exception {
    NfsExportBinding replacement = new NfsExportBinding(binding.getExportId(), binding.getClusterId(),
        binding.getVolumeName(), binding.getBucketName(), binding.getVolumeId(), binding.getBucketId());
    assertEquals(binding.getRootHandle(), replacement.getRootHandle());
    replacement.validateHandle("cluster-a", binding.getRootHandle());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "\t", "\n"})
  void rejectsEmptyConfiguration(String empty) {
    assertThrows(IllegalArgumentException.class,
        () -> new NfsExportBinding(EXPORT_ID, empty, "volume", "bucket", 1, 2));
    assertThrows(IllegalArgumentException.class,
        () -> new NfsExportBinding(EXPORT_ID, "cluster", empty, "bucket", 1, 2));
    assertThrows(IllegalArgumentException.class,
        () -> new NfsExportBinding(EXPORT_ID, "cluster", "volume", empty, 1, 2));
  }
}

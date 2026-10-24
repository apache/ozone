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

package org.apache.hadoop.ozone.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.S3VolumeContext;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests S3 volume selection and delegation through the existing client interface. */
class TestS3ClientProtocol {
  private final ClientProtocol delegate = mock(ClientProtocol.class);

  @ParameterizedTest
  @CsvSource({"ZDU, false", "ZDU, true", "S3_VOLUME_CONTEXT_RESOLUTION, false",
      "S3_VOLUME_CONTEXT_RESOLUTION, true", "S3_IF_NONE_MATCH_ETAG, false", "S3_IF_NONE_MATCH_ETAG, true"})
  void resolvesVolumeForOlderOm(OzoneManagerVersion version, boolean conditional) throws Exception {
    when(delegate.getS3VolumeContext()).thenReturn(volumeContext("tenant-volume"));
    ClientProtocol client = createClient(version);

    if (conditional) {
      client.deleteKey(null, "bucket", "key", false, "etag");
    } else {
      client.deleteKey(null, "bucket", "key", false);
    }

    boolean resolveOnOm = OzoneManagerVersion.S3_VOLUME_CONTEXT_RESOLUTION.isSupportedBy(version);
    String volume = resolveOnOm ? null : "tenant-volume";
    if (conditional) {
      verify(delegate).deleteKey(volume, "bucket", "key", false, "etag");
    } else {
      verify(delegate).deleteKey(volume, "bucket", "key", false);
    }
    verify(delegate, times(resolveOnOm ? 0 : 1)).getS3VolumeContext();
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3v", "tenant-volume"})
  void preservesExplicitVolume(String volume) throws Exception {
    ClientProtocol client = createClient(OzoneManagerVersion.ZDU);

    client.deleteKey(volume, "bucket", "key", true, "etag");

    verify(delegate).deleteKey(volume, "bucket", "key", true, "etag");
    verify(delegate, never()).getS3VolumeContext();
  }

  @Test
  void resolvesTenantVolumeForEachRequest() throws Exception {
    when(delegate.getS3VolumeContext()).thenReturn(volumeContext("tenant-one"), volumeContext("tenant-two"));
    ClientProtocol client = createClient(OzoneManagerVersion.ZDU);

    client.deleteKey(null, "bucket", "key", false);
    client.deleteKey(null, "bucket", "key", false);

    verify(delegate).deleteKey("tenant-one", "bucket", "key", false);
    verify(delegate).deleteKey("tenant-two", "bucket", "key", false);
  }

  @Test
  void propagatesDeleteFailure() throws Exception {
    ClientProtocol client = createClient(OzoneManagerVersion.S3_VOLUME_CONTEXT_RESOLUTION);
    OMException failure = new OMException("Denied", OMException.ResultCodes.PERMISSION_DENIED);
    doThrow(failure).when(delegate).deleteKey(null, "bucket", "key", false, "etag");

    assertThat(assertThrows(OMException.class, () -> client.deleteKey(null, "bucket", "key", false, "etag")))
        .isSameAs(failure);
  }

  @Test
  void doesNotDeleteWhenVolumeResolutionFails() throws Exception {
    ClientProtocol client = createClient(OzoneManagerVersion.ZDU);
    IOException failure = new IOException("Volume lookup failed");
    when(delegate.getS3VolumeContext()).thenThrow(failure);

    assertThat(assertThrows(IOException.class, () -> client.deleteKey(null, "bucket", "key", false)))
        .isSameAs(failure);
    verify(delegate, never()).deleteKey(any(), anyString(), anyString(), anyBoolean());
  }

  @Test
  void forwardsRequestContextAndClose() throws Exception {
    ClientProtocol client = createClient(OzoneManagerVersion.S3_VOLUME_CONTEXT_RESOLUTION);
    ThreadLocal<S3Auth> credentials = new ThreadLocal<>();
    when(delegate.getS3CredentialsProvider()).thenReturn(credentials);

    assertThat(client.getS3CredentialsProvider()).isSameAs(credentials);
    client.clearThreadLocalS3Auth();
    client.clearThreadLocalReadConsistency();
    client.close();

    verify(delegate).clearThreadLocalS3Auth();
    verify(delegate).clearThreadLocalReadConsistency();
    verify(delegate).close();
  }

  @Test
  void usesProxyIdentityForEquality() {
    ClientProtocol client = createClient(OzoneManagerVersion.S3_VOLUME_CONTEXT_RESOLUTION);
    ClientProtocol other = S3ClientProtocol.create(delegate);

    assertThat(client.equals(client)).isTrue();
    assertThat(client.equals(other)).isFalse();
    assertThat(client.equals(delegate)).isFalse();
    assertThat(client.equals(null)).isFalse();
  }

  private ClientProtocol createClient(OzoneManagerVersion version) {
    when(delegate.getOmVersion()).thenReturn(version);
    return S3ClientProtocol.create(delegate);
  }

  private static S3VolumeContext volumeContext(String volume) {
    return new S3VolumeContext(OmVolumeArgs.newBuilder().setVolume(volume).setOwnerName("owner")
        .setAdminName("admin").build(), "owner");
  }
}

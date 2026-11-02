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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.S3VolumeContext;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.invocation.Invocation;

/** Tests transparent delegation for S3 Gateway clients. */
class TestS3ClientProtocol {
  private final ClientProtocol delegate = mock(ClientProtocol.class);
  private final ClientProtocol client = new S3ClientProtocol(delegate);

  private static Stream<Arguments> methods() {
    return Arrays.stream(ClientProtocol.class.getMethods())
        .filter(method -> !Modifier.isStatic(method.getModifiers()))
        .flatMap(method -> Stream.of(false, true).map(reverseBooleans -> Arguments.of(method, reverseBooleans)));
  }

  @ParameterizedTest(name = "{0} [reverseBooleans={1}]")
  @MethodSource("methods")
  void forwardsEveryMethod(Method method, boolean reverseBooleans) throws Exception {
    Class<?>[] types = method.getParameterTypes();
    Object[] args = new Object[types.length];
    for (int i = 0; i < types.length; i++) {
      args[i] = value(types[i], i + 1, reverseBooleans);
    }
    Object expected = value(method.getReturnType(), types.length + 100, reverseBooleans);
    if (method.getReturnType() != void.class) {
      when(method.invoke(delegate, args)).thenReturn(expected);
    }
    clearInvocations(delegate);

    Object actual = method.invoke(client, args);

    if (method.getReturnType().isPrimitive()) {
      assertEquals(expected, actual);
    } else {
      assertSame(expected, actual);
    }
    assertThat(mockingDetails(delegate).getInvocations()).hasSize(1);
    Invocation invocation = mockingDetails(delegate).getInvocations().iterator().next();
    assertEquals(method, invocation.getMethod());
    Object[] forwarded = invocation.getRawArguments();
    assertEquals(args.length, forwarded.length);
    for (int i = 0; i < types.length; i++) {
      if (types[i].isPrimitive()) {
        assertEquals(args[i], forwarded[i]);
      } else {
        assertSame(args[i], forwarded[i]);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = OzoneManagerVersion.class,
      names = {"ZDU", "S3_VOLUME_CONTEXT_RESOLUTION"})
  void resolvesS3ContextVolumeForObjectTagging(OzoneManagerVersion version)
      throws Exception {
    when(delegate.getOmVersion()).thenReturn(version);
    when(delegate.getS3VolumeContext()).thenReturn(new S3VolumeContext(
        OmVolumeArgs.newBuilder().setVolume("tenant-volume")
            .setOwnerName("owner").setAdminName("admin").build(), "owner"));

    client.getObjectTagging(null, "bucket", "key");

    if (version == OzoneManagerVersion.S3_VOLUME_CONTEXT_RESOLUTION) {
      verify(delegate).getObjectTagging(null, "bucket", "key");
      verify(delegate, never()).getS3VolumeContext();
    } else {
      verify(delegate).getObjectTagging("tenant-volume", "bucket", "key");
      verify(delegate).getS3VolumeContext();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void factoryInstallsDelegationOnlyForS3Clients(boolean s3Client) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(S3Auth.S3_AUTH_CHECK, s3Client);
    try (MockedConstruction<RpcClient> clients = mockConstruction(RpcClient.class)) {
      RpcClient raw;
      try (OzoneClient ozoneClient = OzoneClientFactory.getRpcClient(conf)) {
        assertThat(clients.constructed()).hasSize(1);
        raw = clients.constructed().get(0);
        assertSame(ozoneClient.getProxy(), ozoneClient.getObjectStore().getClientProxy());
        if (s3Client) {
          assertThat(ozoneClient.getProxy()).isInstanceOf(S3ClientProtocol.class);
        } else {
          assertSame(raw, ozoneClient.getProxy());
        }
        ozoneClient.getObjectStore().createVolume("volume");
        verify(raw).createVolume("volume");
      }
      verify(raw).close();
    }
  }

  @Test
  void forwardsNullArgumentUnchanged() throws Exception {
    client.deleteKey(null, "bucket", "key", false);

    verify(delegate).deleteKey(null, "bucket", "key", false);
  }

  @Test
  void propagatesExceptionUnchanged() throws Exception {
    IOException failure = new IOException("Lookup failed");
    doThrow(failure).when(delegate).getVolumeDetails("volume");

    assertSame(failure, assertThrows(IOException.class, () -> client.getVolumeDetails("volume")));
  }

  private static Object value(Class<?> type, int index, boolean reverseBooleans) {
    if (type == void.class) {
      return null;
    }
    if (type == String.class) {
      return "value-" + index;
    }
    if (type == boolean.class || type == Boolean.class) {
      return (index % 2 == 0) != reverseBooleans;
    }
    if (type == int.class || type == Integer.class) {
      return index;
    }
    if (type == long.class || type == Long.class) {
      return (long) index;
    }
    if (type == ThreadLocal.class) {
      return new ThreadLocal<>();
    }
    if (type.isEnum()) {
      Object[] constants = type.getEnumConstants();
      return constants[index % constants.length];
    }
    if (type.isArray()) {
      Object array = Array.newInstance(type.getComponentType(), 1);
      Array.set(array, 0, value(type.getComponentType(), index, reverseBooleans));
      return array;
    }
    return mock(type);
  }
}

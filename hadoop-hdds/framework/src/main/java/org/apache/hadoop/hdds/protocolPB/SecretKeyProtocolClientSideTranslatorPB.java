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

package org.apache.hadoop.hdds.protocolPB;

import com.google.common.base.Preconditions;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.SecretKeyProtocol;
import org.apache.hadoop.hdds.protocol.SecretKeyProtocolScm;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos.SCMGetSecretKeyRequest;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos.SCMGetSecretKeyResponse;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos.SCMSecretKeyProtocolService.BlockingInterface;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos.SCMSecretKeyRequest;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos.SCMSecretKeyRequest.Builder;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos.SCMSecretKeyResponse;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos.Type;
import org.apache.hadoop.hdds.scm.proxy.SecretKeyProtocolFailoverProxyProvider;
import org.apache.hadoop.hdds.security.exception.SCMSecretKeyException;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc_.ProtobufHelper;
import org.apache.hadoop.ipc_.ProtocolTranslator;
import org.apache.hadoop.ipc_.RPC;

/**
 * This class is the client-side translator that forwards requests for
 * {@link SecretKeyProtocol} to the server proxy.
 */
public class SecretKeyProtocolClientSideTranslatorPB implements
    SecretKeyProtocol, SecretKeyProtocolScm, ProtocolTranslator, Closeable {

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;
  private final BlockingInterface rpcProxy;

  public SecretKeyProtocolClientSideTranslatorPB(
      SecretKeyProtocolFailoverProxyProvider<? extends BlockingInterface>
          proxyProvider, Class<? extends BlockingInterface> proxyClazz) {
    Preconditions.checkState(proxyProvider != null);
    SecretKeyProtocolFailoverProxyProvider failoverProxyProvider = proxyProvider;
    this.rpcProxy = (BlockingInterface) RetryProxy.create(
        proxyClazz, failoverProxyProvider,
        failoverProxyProvider.getRetryPolicy());
  }

  /**
   * Helper method to wrap the request and send the message.
   */
  private SCMSecretKeyResponse submitRequest(
      Type type,
      Consumer<Builder> builderConsumer) throws IOException {
    final SCMSecretKeyResponse response;
    try {

      Builder builder = SCMSecretKeyRequest.newBuilder()
          .setCmdType(type)
          .setTraceID(TracingUtil.exportCurrentSpan());
      builderConsumer.accept(builder);
      SCMSecretKeyRequest wrapper = builder.build();

      response = rpcProxy.submitRequest(NULL_RPC_CONTROLLER, wrapper);

      handleError(response);

    } catch (ServiceException ex) {
      throw ProtobufHelper.getRemoteException(ex);
    }
    return response;
  }

  private SCMSecretKeyResponse handleError(SCMSecretKeyResponse resp)
      throws SCMSecretKeyException {
    if (resp.getStatus() != SCMSecretKeyProtocolProtos.Status.OK) {
      throw new SCMSecretKeyException(resp.getMessage(),
          SCMSecretKeyException.ErrorCode.values()[resp.getStatus().ordinal()]);
    }
    return resp;
  }

  /**
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   *
   * <p> As noted in {@link AutoCloseable#close()}, cases where the
   * close may fail require careful attention. It is strongly advised
   * to relinquish the underlying resources and to internally
   * <em>mark</em> the {@code Closeable} as closed, prior to throwing
   * the {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public ManagedSecretKey getCurrentSecretKey() throws IOException {
    SCMSecretKeyProtocolProtos.ManagedSecretKey secretKeyProto =
        submitRequest(Type.GetCurrentSecretKey, builder -> {
        }).getCurrentSecretKeyResponseProto().getSecretKey();
    return ManagedSecretKey.fromProtobuf(secretKeyProto);
  }

  @Override
  public boolean checkAndRotate(boolean force) throws IOException {
    SCMSecretKeyProtocolProtos.SCMGetCheckAndRotateRequest request =
        SCMSecretKeyProtocolProtos.SCMGetCheckAndRotateRequest.newBuilder()
            .setForce(force)
            .build();
    boolean checkAndRotateStatus =
        submitRequest(Type.CheckAndRotate, builder ->
            builder.setCheckAndRotateRequest(request))
            .getCheckAndRotateResponseProto().getStatus();

    return checkAndRotateStatus;
  }

  @Override
  public ManagedSecretKey getSecretKey(UUID id) throws IOException {
    SCMGetSecretKeyRequest request = SCMGetSecretKeyRequest.newBuilder()
        .setSecretKeyId(HddsProtos.UUID.newBuilder()
            .setMostSigBits(id.getMostSignificantBits())
            .setLeastSigBits(id.getLeastSignificantBits())).build();
    SCMGetSecretKeyResponse response = submitRequest(Type.GetSecretKey,
        builder -> builder.setGetSecretKeyRequest(request))
        .getGetSecretKeyResponseProto();

    return response.hasSecretKey() ?
        ManagedSecretKey.fromProtobuf(response.getSecretKey()) : null;
  }

  @Override
  public List<ManagedSecretKey> getAllSecretKeys() throws IOException {
    List<SCMSecretKeyProtocolProtos.ManagedSecretKey> secretKeysList =
        submitRequest(Type.GetAllSecretKeys, builder -> {
        }).getSecretKeysListResponseProto().getSecretKeysList();
    return secretKeysList.stream()
        .map(ManagedSecretKey::fromProtobuf)
        .collect(Collectors.toList());
  }

  /**
   * Return the proxy object underlying this protocol translator.
   *
   * @return the proxy object underlying this protocol translator.
   */
  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }
}

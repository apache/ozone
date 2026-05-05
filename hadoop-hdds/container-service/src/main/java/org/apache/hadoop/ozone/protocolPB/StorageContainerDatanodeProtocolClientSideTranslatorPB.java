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

package org.apache.hadoop.ozone.protocolPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ExtendedDatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMDatanodeRequest;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMDatanodeRequest.Builder;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMDatanodeResponse;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisterRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.Type;
import org.apache.hadoop.ipc_.ProtobufHelper;
import org.apache.hadoop.ipc_.ProtocolTranslator;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;

/**
 * This class is the client-side translator to translate the requests made on
 * the {@link StorageContainerDatanodeProtocol} interface to the RPC server
 * implementing {@link StorageContainerDatanodeProtocolPB}.
 */
public class StorageContainerDatanodeProtocolClientSideTranslatorPB
    implements StorageContainerDatanodeProtocol, ProtocolTranslator, Closeable {

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;
  private final StorageContainerDatanodeProtocolPB rpcProxy;

  /**
   * Constructs a Client side interface that calls into SCM datanode protocol.
   *
   * @param rpcProxy - Proxy for RPC.
   */
  public StorageContainerDatanodeProtocolClientSideTranslatorPB(
      StorageContainerDatanodeProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  /**
   * Closes this stream and releases any system resources associated with it. If
   * the stream is already closed then invoking this method has no effect.
   * <p>
   * <p> As noted in {@link AutoCloseable#close()}, cases where the close may
   * fail require careful attention. It is strongly advised to relinquish the
   * underlying resources and to internally <em>mark</em> the {@code Closeable}
   * as closed, prior to throwing the {@code IOException}.
   */
  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
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

  /**
   * Helper method to wrap the request and send the message.
   */
  private SCMDatanodeResponse submitRequest(Type type,
      Consumer<SCMDatanodeRequest.Builder> builderConsumer) throws IOException {
    final SCMDatanodeResponse response;
    try {
      Builder builder = SCMDatanodeRequest.newBuilder()
          .setCmdType(type);
      builderConsumer.accept(builder);
      SCMDatanodeRequest wrapper = builder.build();

      response = rpcProxy.submitRequest(NULL_RPC_CONTROLLER, wrapper);
    } catch (ServiceException ex) {
      throw ProtobufHelper.getRemoteException(ex);
    }
    return response;
  }

  /**
   * Returns SCM version.
   *
   * @return Version info.
   */
  @Override
  public SCMVersionResponseProto getVersion(SCMVersionRequestProto
      request) throws IOException {
    return submitRequest(Type.GetVersion,
        (builder) -> builder
            .setGetVersionRequest(SCMVersionRequestProto.newBuilder().build()))
        .getGetVersionResponse();
  }

  /**
   * Send by datanode to SCM.
   *
   * @param heartbeat node heartbeat
   * @throws IOException
   */

  @Override
  public SCMHeartbeatResponseProto sendHeartbeat(
      SCMHeartbeatRequestProto heartbeat) throws IOException {
    return submitRequest(Type.SendHeartbeat,
        (builder) -> builder.setSendHeartbeatRequest(heartbeat))
        .getSendHeartbeatResponse();
  }

  /**
   * Register Datanode.
   *
   * @param extendedDatanodeDetailsProto - extended Datanode Details
   * @param nodeReport - Node Report.
   * @param containerReportsRequestProto - Container Reports.
   * @param layoutInfo - Layout Version Information.
   * @return SCM Command.
   */
  @Override
  public SCMRegisteredResponseProto register(
      ExtendedDatanodeDetailsProto extendedDatanodeDetailsProto,
      NodeReportProto nodeReport,
      ContainerReportsProto containerReportsRequestProto,
      PipelineReportsProto pipelineReportsProto,
      LayoutVersionProto layoutInfo)
      throws IOException {
    SCMRegisterRequestProto.Builder req =
        SCMRegisterRequestProto.newBuilder();
    req.setExtendedDatanodeDetails(extendedDatanodeDetailsProto);
    req.setContainerReport(containerReportsRequestProto);
    req.setPipelineReports(pipelineReportsProto);
    req.setNodeReport(nodeReport);
    if (layoutInfo != null) {
      req.setDataNodeLayoutVersion(layoutInfo);
    }
    return submitRequest(Type.Register,
        (builder) -> builder.setRegisterRequest(req))
        .getRegisterResponse();
  }
}

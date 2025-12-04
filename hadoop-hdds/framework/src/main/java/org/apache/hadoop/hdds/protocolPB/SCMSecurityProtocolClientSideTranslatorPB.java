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

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.OzoneManagerDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ScmNodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetAllRootCaCertificatesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCACertificateRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertificateRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetDataNodeCertRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetOMCertRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetSCMCertRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMListCACertificateRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMListCertificateRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMRemoveExpiredCertificatesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMSecurityRequest;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMSecurityRequest.Builder;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMSecurityResponse;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.Type;
import org.apache.hadoop.hdds.scm.proxy.SCMSecurityProtocolFailoverProxyProvider;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc_.ProtobufHelper;
import org.apache.hadoop.ipc_.ProtocolTranslator;
import org.apache.hadoop.ipc_.RPC;

/**
 * This class is the client-side translator that forwards requests for
 * {@link SCMSecurityProtocol} to the {@link SCMSecurityProtocolPB} proxy.
 */
public class SCMSecurityProtocolClientSideTranslatorPB implements
    SCMSecurityProtocol, ProtocolTranslator, Closeable {

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;
  private final SCMSecurityProtocolPB rpcProxy;

  public SCMSecurityProtocolClientSideTranslatorPB(
      SCMSecurityProtocolFailoverProxyProvider failoverProxyProvider) {
    Objects.requireNonNull(failoverProxyProvider,
        "failoverProxyProvider == null");
    this.rpcProxy = (SCMSecurityProtocolPB) RetryProxy.create(
        SCMSecurityProtocolPB.class, failoverProxyProvider,
        failoverProxyProvider.getRetryPolicy());
  }

  /**
   * Helper method to wrap the request and send the message.
   */
  private SCMSecurityResponse submitRequest(Type type,
      Consumer<Builder> builderConsumer) throws IOException {
    final SCMSecurityResponse response;
    try {

      Builder builder = SCMSecurityRequest.newBuilder()
          .setCmdType(type)
          .setTraceID(TracingUtil.exportCurrentSpan());
      builderConsumer.accept(builder);
      SCMSecurityRequest wrapper = builder.build();

      response = rpcProxy.submitRequest(NULL_RPC_CONTROLLER, wrapper);

      handleError(response);

    } catch (ServiceException ex) {
      throw ProtobufHelper.getRemoteException(ex);
    }
    return response;
  }

  /**
   * If response is not successful, throw exception.
   * @param resp - SCMSecurityResponse
   * @return if response is success, return response, else throw exception.
   * @throws SCMSecurityException
   */
  private SCMSecurityResponse handleError(SCMSecurityResponse resp)
      throws SCMSecurityException {
    if (resp.getStatus() != SCMSecurityProtocolProtos.Status.OK) {
      throw new SCMSecurityException(resp.getMessage(),
          SCMSecurityException.ErrorCode.values()[resp.getStatus().ordinal()]);
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

  /**
   * Get SCM signed certificate for DataNode.
   *
   * @param dataNodeDetails - DataNode Details.
   * @param certSignReq     - Certificate signing request.
   * @return byte[]         - SCM signed certificate.
   */
  @Override
  public String getDataNodeCertificate(DatanodeDetailsProto dataNodeDetails,
      String certSignReq) throws IOException {
    return getDataNodeCertificateChain(dataNodeDetails, certSignReq)
        .getX509Certificate();
  }

  /**
   * Get SCM signed certificate for OM.
   *
   * @param omDetails   - OzoneManager Details.
   * @param certSignReq - Certificate signing request.
   * @return byte[]         - SCM signed certificate.
   */
  @Override
  public String getOMCertificate(OzoneManagerDetailsProto omDetails,
      String certSignReq) throws IOException {
    return getOMCertChain(omDetails, certSignReq).getX509Certificate();
  }

  /**
   * Get SCM signed certificate.
   *
   * @param nodeDetails - Node Details.
   * @param certSignReq  - Certificate signing request.
   * @return String      - pem encoded SCM signed
   *                         certificate.
   */
  @Override
  public String getCertificate(NodeDetailsProto nodeDetails,
      String certSignReq) throws IOException {
    return getCertificateChain(nodeDetails, certSignReq)
        .getX509Certificate();
  }

  /**
   * Get signed certificate for SCM node.
   *
   * @param scmNodeDetails  - SCM Node Details.
   * @param certSignReq     - Certificate signing request.
   * @return String         - pem encoded SCM signed
   *                          certificate.
   */
  @Override
  public String getSCMCertificate(ScmNodeDetailsProto scmNodeDetails,
      String certSignReq) throws IOException {
    return getSCMCertChain(scmNodeDetails, certSignReq, false)
        .getX509Certificate();
  }

  /**
   * Get signed certificate for SCM node.
   *
   * @param scmNodeDetails  - SCM Node Details.
   * @param certSignReq     - Certificate signing request.
   * @param renew           - Whether SCM is trying to renew its certificate
   * @return String         - pem encoded SCM signed
   *                          certificate.
   */
  @Override
  public String getSCMCertificate(ScmNodeDetailsProto scmNodeDetails,
      String certSignReq, boolean renew) throws IOException {
    return getSCMCertChain(scmNodeDetails, certSignReq, renew)
        .getX509Certificate();
  }

  /**
   * Get signed certificate for SCM node and root CA certificate.
   *
   * @param scmNodeDetails   - SCM Node Details.
   * @param certSignReq      - Certificate signing request.
   * @return SCMGetCertResponseProto  - SCMGetCertResponseProto which holds
   * signed certificate and root CA certificate.
   */
  public SCMGetCertResponseProto getSCMCertChain(
      ScmNodeDetailsProto scmNodeDetails, String certSignReq, boolean isRenew)
      throws IOException {
    SCMGetSCMCertRequestProto request =
        SCMGetSCMCertRequestProto.newBuilder()
            .setCSR(certSignReq)
            .setScmDetails(scmNodeDetails)
            .setRenew(isRenew)
            .build();
    return submitRequest(Type.GetSCMCertificate,
        builder -> builder.setGetSCMCertificateRequest(request))
        .getGetCertResponseProto();
  }

  /**
   * Get SCM signed certificate for OM.
   *
   * @param omDetails   - OzoneManager Details.
   * @param certSignReq - Certificate signing request.
   * @return byte[]         - SCM signed certificate.
   */
  public SCMGetCertResponseProto getOMCertChain(
      OzoneManagerDetailsProto omDetails, String certSignReq)
      throws IOException {
    SCMGetOMCertRequestProto request = SCMGetOMCertRequestProto
        .newBuilder()
        .setCSR(certSignReq)
        .setOmDetails(omDetails)
        .build();
    return submitRequest(Type.GetOMCertificate,
        builder -> builder.setGetOMCertRequest(request))
        .getGetCertResponseProto();
  }

  /**
   * Get SCM signed certificate with given serial id. Throws exception if
   * certificate is not found.
   *
   * @param certSerialId - Certificate serial id.
   * @return string         - pem encoded certificate.
   */
  @Override
  public String getCertificate(String certSerialId) throws IOException {
    SCMGetCertificateRequestProto request = SCMGetCertificateRequestProto
        .newBuilder()
        .setCertSerialId(certSerialId)
        .build();
    return submitRequest(Type.GetCertificate,
        builder -> builder.setGetCertificateRequest(request))
        .getGetCertResponseProto()
        .getX509Certificate();
  }

  /**
   * Get SCM signed certificate for Datanode.
   *
   * @param dnDetails   - Datanode Details.
   * @param certSignReq - Certificate signing request.
   * @return byte[]         - SCM signed certificate.
   */
  public SCMGetCertResponseProto getDataNodeCertificateChain(
      DatanodeDetailsProto dnDetails, String certSignReq)
      throws IOException {

    SCMGetDataNodeCertRequestProto request =
        SCMGetDataNodeCertRequestProto.newBuilder()
            .setCSR(certSignReq)
            .setDatanodeDetails(dnDetails)
            .build();
    return submitRequest(Type.GetDataNodeCertificate,
        builder -> builder.setGetDataNodeCertRequest(request))
        .getGetCertResponseProto();
  }

  /**
   * Get SCM signed certificate.
   *
   * @param nodeDetails   - Node Details.
   * @param certSignReq - Certificate signing request.
   * @return byte[]         - SCM signed certificate.
   */
  public SCMGetCertResponseProto getCertificateChain(
      NodeDetailsProto nodeDetails, String certSignReq)
      throws IOException {

    SCMGetCertRequestProto request =
        SCMGetCertRequestProto.newBuilder()
            .setCSR(certSignReq)
            .setNodeDetails(nodeDetails)
            .build();
    return submitRequest(Type.GetCert,
        builder -> builder.setGetCertRequest(request))
        .getGetCertResponseProto();
  }

  /**
   * Get CA certificate.
   *
   * @return serial   - Root certificate.
   */
  @Override
  public String getCACertificate() throws IOException {
    return getCACert().getX509Certificate();
  }

  public SCMGetCertResponseProto getCACert() throws IOException {
    SCMGetCACertificateRequestProto protoIns = SCMGetCACertificateRequestProto
        .getDefaultInstance();
    return submitRequest(Type.GetCACertificate,
        builder -> builder.setGetCACertificateRequest(protoIns))
        .getGetCertResponseProto();
  }

  /**
   * @param role          - node type: OM/SCM/DN.
   * @param startSerialId - start cert serial id.
   * @param count         - max number of certificates returned in a batch.
   * @throws IOException
   */
  @Override
  public List<String> listCertificate(HddsProtos.NodeType role,
      long startSerialId, int count) throws IOException {
    SCMListCertificateRequestProto protoIns = SCMListCertificateRequestProto
        .newBuilder()
        .setRole(role)
        .setStartCertId(startSerialId)
        .setCount(count)
        .build();
    return submitRequest(Type.ListCertificate,
        builder -> builder.setListCertificateRequest(protoIns))
        .getListCertificateResponseProto().getCertificatesList();
  }

  @Override
  public String getRootCACertificate() throws IOException {
    SCMGetCACertificateRequestProto protoIns = SCMGetCACertificateRequestProto
        .getDefaultInstance();
    return submitRequest(Type.GetRootCACertificate,
        builder -> builder.setGetCACertificateRequest(protoIns))
        .getGetCertResponseProto().getX509RootCACertificate();
  }

  @Override
  public List<String> listCACertificate() throws IOException {
    SCMListCACertificateRequestProto proto =
        SCMListCACertificateRequestProto.getDefaultInstance();
    return submitRequest(Type.ListCACertificate,
        builder -> builder.setListCACertificateRequestProto(proto))
        .getListCertificateResponseProto().getCertificatesList();
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

  @Override
  public List<String> getAllRootCaCertificates() throws IOException {
    SCMGetAllRootCaCertificatesRequestProto protoIns =
        SCMGetAllRootCaCertificatesRequestProto.getDefaultInstance();
    return submitRequest(Type.GetAllRootCaCertificates,
        builder -> builder.setGetAllRootCaCertificatesRequestProto(protoIns))
        .getAllRootCaCertificatesResponseProto()
        .getAllX509RootCaCertificatesList();
  }

  @Override
  public List<String> removeExpiredCertificates() throws IOException {
    SCMRemoveExpiredCertificatesRequestProto protoIns =
        SCMRemoveExpiredCertificatesRequestProto.getDefaultInstance();
    return submitRequest(Type.RemoveExpiredCertificates,
        builder -> builder.setRemoveExpiredCertificatesRequestProto(protoIns))
        .getRemoveExpiredCertificatesResponseProto()
        .getRemovedExpiredCertificatesList();
  }
}

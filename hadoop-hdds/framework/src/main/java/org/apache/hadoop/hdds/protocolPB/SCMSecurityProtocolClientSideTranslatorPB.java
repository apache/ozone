/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.protocolPB;

import java.io.Closeable;
import java.io.IOException;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.CRLInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.OzoneManagerDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ScmNodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetSCMCertRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCACertificateRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertificateRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCrlsRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetDataNodeCertRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMListCACertificateRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetLatestCrlIdRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMListCertificateRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMRevokeCertificatesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMRevokeCertificatesRequestProto.Reason;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMSecurityRequest;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMSecurityRequest.Builder;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMSecurityResponse;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.Type;
import org.apache.hadoop.hdds.scm.proxy.SCMSecurityProtocolFailoverProxyProvider;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import static org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetOMCertRequestProto;

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
  private SCMSecurityProtocolFailoverProxyProvider failoverProxyProvider;

  public SCMSecurityProtocolClientSideTranslatorPB(
      SCMSecurityProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  public SCMSecurityProtocolClientSideTranslatorPB(
      SCMSecurityProtocolFailoverProxyProvider proxyProvider) {
    Preconditions.checkState(proxyProvider != null);
    this.failoverProxyProvider = proxyProvider;
    this.rpcProxy = (SCMSecurityProtocolPB) RetryProxy.create(
        SCMSecurityProtocolPB.class, failoverProxyProvider,
        failoverProxyProvider.getRetryPolicy());
  }

  /**
   * Helper method to wrap the request and send the message.
   */
  private SCMSecurityResponse submitRequest(
      SCMSecurityProtocolProtos.Type type,
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
   * Get signed certificate for SCM node.
   *
   * @param scmNodeDetails  - SCM Node Details.
   * @param certSignReq     - Certificate signing request.
   * @return String         - pem encoded SCM signed
   *                          certificate.
   */
  public String getSCMCertificate(ScmNodeDetailsProto scmNodeDetails,
      String certSignReq) throws IOException {
    return getSCMCertChain(scmNodeDetails, certSignReq).getX509Certificate();
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
      ScmNodeDetailsProto scmNodeDetails, String certSignReq)
      throws IOException {
    SCMGetSCMCertRequestProto request =
        SCMGetSCMCertRequestProto.newBuilder()
            .setCSR(certSignReq)
            .setScmDetails(scmNodeDetails)
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
   *
   * @param role            - node type: OM/SCM/DN.
   * @param startSerialId   - start cert serial id.
   * @param count           - max number of certificates returned in a batch.
   * @param isRevoked       - whether return revoked cert only.
   * @return
   * @throws IOException
   */
  @Override
  public List<String> listCertificate(HddsProtos.NodeType role,
      long startSerialId, int count, boolean isRevoked) throws IOException {
    SCMListCertificateRequestProto protoIns = SCMListCertificateRequestProto
        .newBuilder()
        .setRole(role)
        .setStartCertId(startSerialId)
        .setCount(count)
        .setIsRevoked(isRevoked)
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

  @Override
  public List<CRLInfo> getCrls(List<Long> crlIds) throws IOException {
    SCMGetCrlsRequestProto protoIns = SCMGetCrlsRequestProto
        .newBuilder()
        .addAllCrlId(crlIds)
        .build();
    List<CRLInfoProto> crlInfoProtoList = submitRequest(Type.GetCrls,
        builder -> builder.setGetCrlsRequest(protoIns))
        .getGetCrlsResponseProto().getCrlInfosList();
    List<CRLInfo> result = new ArrayList<>();
    for (CRLInfoProto crlProto : crlInfoProtoList) {
      try {
        CRLInfo crlInfo = CRLInfo.fromProtobuf(crlProto);
        result.add(crlInfo);
      } catch (CRLException | CertificateException e) {
        throw new SCMSecurityException("Fail to parse CRL info", e);
      }
    }
    return result;
  }

  @Override
  public long getLatestCrlId() throws IOException {
    SCMGetLatestCrlIdRequestProto protoIns =  SCMGetLatestCrlIdRequestProto
        .getDefaultInstance();
    return submitRequest(Type.GetLatestCrlId,
        builder -> builder.setGetLatestCrlIdRequest(protoIns))
        .getGetLatestCrlIdResponseProto().getCrlId();
  }

  @Override
  public long revokeCertificates(List<String> certIds, int reason,
      long revocationTime) throws IOException {
    SCMRevokeCertificatesRequestProto req = SCMRevokeCertificatesRequestProto
        .newBuilder().addAllCertIds(certIds)
        .setReason(Reason.valueOf(reason))
        .setRevokeTime(revocationTime).build();
    return submitRequest(Type.RevokeCertificates,
        builder->builder.setRevokeCertificatesRequest(req))
        .getRevokeCertificatesResponseProto().getCrlId();
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
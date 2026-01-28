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

package org.apache.hadoop.hdds.scm.protocol;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetAllRootCaCertificatesResponseProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto.ResponseCode;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertificateRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetDataNodeCertRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetOMCertRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetSCMCertRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMListCertificateRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMListCertificateResponseProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMRemoveExpiredCertificatesResponseProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMSecurityRequest;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMSecurityResponse;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.Status;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolPB;
import org.apache.hadoop.hdds.scm.ha.RatisUtil;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.server.OzoneProtocolMessageDispatcher;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link SCMSecurityProtocolPB} to the {@link
 * SCMSecurityProtocol} server implementation.
 */
public class SCMSecurityProtocolServerSideTranslatorPB
    implements SCMSecurityProtocolPB {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMSecurityProtocolServerSideTranslatorPB.class);

  private final SCMSecurityProtocol impl;
  private final StorageContainerManager scm;
  private static final String ROLE_TYPE = "SCM";

  private OzoneProtocolMessageDispatcher<SCMSecurityRequest,
      SCMSecurityResponse, SCMSecurityProtocolProtos.Type>
      dispatcher;

  public SCMSecurityProtocolServerSideTranslatorPB(SCMSecurityProtocol impl,
      StorageContainerManager storageContainerManager,
      ProtocolMessageMetrics messageMetrics) {
    this.impl = impl;
    this.scm = storageContainerManager;
    this.dispatcher =
        new OzoneProtocolMessageDispatcher<>("ScmSecurityProtocol",
            messageMetrics, LOG);
  }

  @Override
  public SCMSecurityResponse submitRequest(RpcController controller,
      SCMSecurityRequest request) throws ServiceException {
    if (!scm.checkLeader()) {
      RatisUtil.checkRatisException(
          scm.getScmHAManager().getRatisServer().triggerNotLeaderException(),
          scm.getSecurityProtocolRpcPort(), scm.getScmId(), scm.getHostname(), ROLE_TYPE);
    }
    return dispatcher.processRequest(request, this::processRequest,
        request.getCmdType(), request.getTraceID());
  }

  public SCMSecurityResponse processRequest(SCMSecurityRequest request)
      throws ServiceException {
    SCMSecurityResponse.Builder scmSecurityResponse =
        SCMSecurityResponse.newBuilder().setCmdType(request.getCmdType())
            .setStatus(Status.OK);
    try {
      switch (request.getCmdType()) {
      case GetCertificate:
        return scmSecurityResponse.setGetCertResponseProto(
            getCertificate(request.getGetCertificateRequest())).build();
      case GetCACertificate:
        return scmSecurityResponse.setGetCertResponseProto(
            getCACertificate(request.getGetCACertificateRequest())).build();
      case GetOMCertificate:
        return scmSecurityResponse.setGetCertResponseProto(
            getOMCertificate(request.getGetOMCertRequest()))
            .build();
      case GetDataNodeCertificate:
        return scmSecurityResponse.setGetCertResponseProto(
            getDataNodeCertificate(request.getGetDataNodeCertRequest()))
            .build();
      case ListCertificate:
        return scmSecurityResponse.setListCertificateResponseProto(
            listCertificate(request.getListCertificateRequest()))
            .build();
      case GetSCMCertificate:
        return scmSecurityResponse.setGetCertResponseProto(getSCMCertificate(
            request.getGetSCMCertificateRequest())).build();
      case GetRootCACertificate:
        return scmSecurityResponse.setGetCertResponseProto(
            getRootCACertificate()).build();
      case ListCACertificate:
        return scmSecurityResponse.setListCertificateResponseProto(
            listCACertificate()).build();
      case GetCrls:
      case GetLatestCrlId:
      case RevokeCertificates:
        return scmSecurityResponse
            .setStatus(Status.INTERNAL_ERROR)
            .setMessage("Unsupported operation.")
            .setSuccess(false)
            .build();
      case GetCert:
        return scmSecurityResponse.setGetCertResponseProto(
                getCertificate(request.getGetCertRequest()))
            .build();
      case GetAllRootCaCertificates:
        return scmSecurityResponse
            .setAllRootCaCertificatesResponseProto(getAllRootCa())
            .build();
      case RemoveExpiredCertificates:
        return scmSecurityResponse
            .setRemoveExpiredCertificatesResponseProto(
                removeExpiredCertificates())
            .build();

      default:
        throw new IllegalArgumentException(
            "Unknown request type: " + request.getCmdType());
      }
    } catch (IOException e) {
      RatisUtil.checkRatisException(e, scm.getSecurityProtocolRpcPort(),
          scm.getScmId(), scm.getHostname(), ROLE_TYPE);
      scmSecurityResponse.setSuccess(false);
      scmSecurityResponse.setStatus(exceptionToResponseStatus(e));
      // If actual cause is set in SCMSecurityException, set message with
      // actual cause message.
      if (e.getMessage() != null) {
        scmSecurityResponse.setMessage(e.getMessage());
      } else {
        if (e.getCause() != null && e.getCause().getMessage() != null) {
          scmSecurityResponse.setMessage(e.getCause().getMessage());
        }
      }
      return scmSecurityResponse.build();
    }
  }

  /**
   * Convert exception to corresponsing status.
   * @param ex
   * @return SCMSecurityProtocolProtos.Status code of the error.
   */
  private Status exceptionToResponseStatus(IOException ex) {
    if (ex instanceof SCMSecurityException) {
      return Status.values()[
          ((SCMSecurityException) ex).getErrorCode().ordinal()];
    } else {
      return Status.INTERNAL_ERROR;
    }
  }

  /**
   * Get SCM signed certificate for DataNode.
   *
   * @param request
   * @return SCMGetDataNodeCertResponseProto.
   */

  public SCMGetCertResponseProto getDataNodeCertificate(
      SCMGetDataNodeCertRequestProto request)
      throws IOException {

    String certificate = impl
        .getDataNodeCertificate(request.getDatanodeDetails(),
            request.getCSR());
    SCMGetCertResponseProto.Builder builder =
        SCMGetCertResponseProto
            .newBuilder()
            .setResponseCode(ResponseCode.success)
            .setX509Certificate(certificate)
            .setX509CACertificate(impl.getCACertificate());
    setRootCAIfNeeded(builder);

    return builder.build();

  }

  /**
   * Get SCM signed certificate.
   *
   * @param request
   * @return SCMGetCertResponseProto.
   */
  public SCMGetCertResponseProto getCertificate(
      SCMGetCertRequestProto request) throws IOException {
    String certificate = impl
        .getCertificate(request.getNodeDetails(),
            request.getCSR());
    SCMGetCertResponseProto.Builder builder =
        SCMGetCertResponseProto
            .newBuilder()
            .setResponseCode(ResponseCode.success)
            .setX509Certificate(certificate)
            .setX509CACertificate(impl.getCACertificate());
    setRootCAIfNeeded(builder);

    return builder.build();
  }

  /**
   * Get signed certificate for SCM.
   *
   * @param request - SCMGetSCMCertRequestProto
   * @return SCMGetCertResponseProto.
   */

  public SCMGetCertResponseProto getSCMCertificate(
      SCMGetSCMCertRequestProto request)
      throws IOException {

    if (!scm.getScmStorageConfig().isSCMHAEnabled()) {
      throw createNotHAException();
    }
    String certificate = impl.getSCMCertificate(request.getScmDetails(),
        request.getCSR(), request.hasRenew() && request.getRenew());
    SCMGetCertResponseProto.Builder builder =
        SCMGetCertResponseProto
            .newBuilder()
            .setResponseCode(ResponseCode.success)
            .setX509Certificate(certificate)
            .setX509CACertificate(impl.getRootCACertificate())
            .setX509RootCACertificate(impl.getRootCACertificate());

    return builder.build();

  }

  /**
   * Get SCM signed certificate for OzoneManager.
   *
   * @param request
   * @return SCMGetCertResponseProto.
   */
  public SCMGetCertResponseProto getOMCertificate(
      SCMGetOMCertRequestProto request) throws IOException {
    String certificate = impl
        .getOMCertificate(request.getOmDetails(),
            request.getCSR());
    SCMGetCertResponseProto.Builder builder =
        SCMGetCertResponseProto
            .newBuilder()
            .setResponseCode(ResponseCode.success)
            .setX509Certificate(certificate)
            .setX509CACertificate(impl.getCACertificate());
    setRootCAIfNeeded(builder);
    return builder.build();

  }

  public SCMGetCertResponseProto getCertificate(
      SCMGetCertificateRequestProto request) throws IOException {

    String certificate = impl.getCertificate(request.getCertSerialId());
    SCMGetCertResponseProto.Builder builder =
        SCMGetCertResponseProto
            .newBuilder()
            .setResponseCode(ResponseCode.success)
            .setX509Certificate(certificate);
    return builder.build();

  }

  public SCMGetCertResponseProto getCACertificate(
      SCMSecurityProtocolProtos.SCMGetCACertificateRequestProto request)
      throws IOException {

    String certificate = impl.getCACertificate();
    SCMGetCertResponseProto.Builder builder =
        SCMGetCertResponseProto
            .newBuilder()
            .setResponseCode(ResponseCode.success)
            .setX509Certificate(certificate)
            .setX509CACertificate(certificate);
    setRootCAIfNeeded(builder);
    return builder.build();

  }

  public SCMListCertificateResponseProto listCertificate(
      SCMListCertificateRequestProto request) throws IOException {
    List<String> certs = impl.listCertificate(request.getRole(),
        request.getStartCertId(), request.getCount());

    SCMListCertificateResponseProto.Builder builder =
        SCMListCertificateResponseProto
            .newBuilder()
            .setResponseCode(SCMListCertificateResponseProto
                .ResponseCode.success)
            .addAllCertificates(certs);
    return builder.build();

  }

  public SCMGetCertResponseProto getRootCACertificate() throws IOException {
    if (scm.getScmStorageConfig().checkPrimarySCMIdInitialized()) {
      throw createNotHAException();
    }
    String rootCACertificate = impl.getRootCACertificate();
    SCMGetCertResponseProto.Builder builder =
        SCMGetCertResponseProto
            .newBuilder()
            .setResponseCode(ResponseCode.success)
            .setX509Certificate(rootCACertificate)
            .setX509RootCACertificate(rootCACertificate);
    return builder.build();
  }

  public SCMListCertificateResponseProto listCACertificate()
      throws IOException {

    List<String> certs = impl.listCACertificate();

    SCMListCertificateResponseProto.Builder builder =
        SCMListCertificateResponseProto
            .newBuilder()
            .setResponseCode(SCMListCertificateResponseProto
                .ResponseCode.success)
            .addAllCertificates(certs);
    return builder.build();

  }

  private SCMSecurityException createNotHAException() {
    return new SCMSecurityException("SCM is not Ratis enabled. Enable ozone" +
        ".scm.ratis.enable config");
  }

  public SCMGetAllRootCaCertificatesResponseProto getAllRootCa()
      throws IOException {
    return SCMGetAllRootCaCertificatesResponseProto.newBuilder()
        .addAllAllX509RootCaCertificates(impl.getAllRootCaCertificates())
        .build();
  }

  private void setRootCAIfNeeded(SCMGetCertResponseProto.Builder builder)
      throws IOException {
    if (scm.getScmStorageConfig().checkPrimarySCMIdInitialized()) {
      builder.setX509RootCACertificate(impl.getRootCACertificate());
    }
  }

  public SCMRemoveExpiredCertificatesResponseProto removeExpiredCertificates()
      throws IOException {
    return SCMRemoveExpiredCertificatesResponseProto.newBuilder()
        .addAllRemovedExpiredCertificates(impl.removeExpiredCertificates())
        .build();
  }
}

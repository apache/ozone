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
package org.apache.hadoop.hdds.scm.protocol;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertResponseProto.ResponseCode;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetCertificateRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetDataNodeCertRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetOMCertRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMGetSCMCertRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMListCertificateRequestProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMListCertificateResponseProto;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMSecurityRequest;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.SCMSecurityResponse;
import org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.Status;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolPB;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.server.OzoneProtocolMessageDispatcher;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;

import com.google.protobuf.ProtocolMessageEnum;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.protocol.proto.SCMSecurityProtocolProtos.Type.GetSCMCertificate;

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

  private OzoneProtocolMessageDispatcher<SCMSecurityRequest,
      SCMSecurityResponse, ProtocolMessageEnum>
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
      throw new ServiceException(scm.getScmHAManager()
          .getRatisServer()
          .triggerNotLeaderException());
    }
    return dispatcher.processRequest(request, this::processRequest,
        request.getCmdType(), request.getTraceID());
  }

  public SCMSecurityResponse processRequest(SCMSecurityRequest request) {
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
      default:
        throw new IllegalArgumentException(
            "Unknown request type: " + request.getCmdType());
      }
    } catch (IOException e) {
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
            .setX509CACertificate(impl.getCACertificate())
            .setX509RootCACertificate(impl.getRootCACertificate());

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

    String certificate = impl.getSCMCertificate(request.getScmDetails(),
        request.getCSR());
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
            .setX509CACertificate(impl.getCACertificate())
            .setX509RootCACertificate(impl.getRootCACertificate());
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
            .setX509CACertificate(certificate)
            .setX509RootCACertificate(impl.getRootCACertificate());
    return builder.build();

  }

  public SCMListCertificateResponseProto listCertificate(
      SCMListCertificateRequestProto request) throws IOException {
    List<String> certs = impl.listCertificate(request.getRole(),
        request.getStartCertId(), request.getCount(), request.getIsRevoked());

    SCMListCertificateResponseProto.Builder builder =
        SCMListCertificateResponseProto
            .newBuilder()
            .setResponseCode(SCMListCertificateResponseProto
                .ResponseCode.success)
            .addAllCertificates(certs);
    return builder.build();


  }


  public SCMGetCertResponseProto getRootCACertificate() throws IOException {
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


}
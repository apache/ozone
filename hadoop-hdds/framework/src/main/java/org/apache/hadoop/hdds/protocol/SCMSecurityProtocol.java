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

package org.apache.hadoop.hdds.protocol;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.OzoneManagerDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ScmNodeDetailsProto;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.security.KerberosInfo;

/**
 * The protocol used to perform security related operations with SCM.
 */
@KerberosInfo(
    serverPrincipal = ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface SCMSecurityProtocol {

  @SuppressWarnings("checkstyle:ConstantName")
  /**
   * Version 1: Initial version.
   */
  long versionID = 1L;

  /**
   * Get SCM signed certificate for DataNode.
   *
   * @param dataNodeDetails - DataNode Details.
   * @param certSignReq     - Certificate signing request.
   * @return byte[]         - SCM signed certificate.
   */
  String getDataNodeCertificate(
      DatanodeDetailsProto dataNodeDetails,
      String certSignReq) throws IOException;

  /**
   * Get SCM signed certificate for OM.
   *
   * @param omDetails       - DataNode Details.
   * @param certSignReq     - Certificate signing request.
   * @return String         - pem encoded SCM signed
   *                          certificate.
   */
  String getOMCertificate(OzoneManagerDetailsProto omDetails,
      String certSignReq) throws IOException;


  /**
   * Get signed certificate for SCM.
   *
   * @param scmNodeDetails  - SCM Node Details.
   * @param certSignReq     - Certificate signing request.
   * @return String         - pem encoded SCM signed
   *                          certificate.
   */
  String getSCMCertificate(ScmNodeDetailsProto scmNodeDetails,
      String certSignReq) throws IOException;

  /**
   * Get signed certificate for SCM.
   *
   * @param scmNodeDetails  - SCM Node Details.
   * @param certSignReq     - Certificate signing request.
   * @param isRenew         - if SCM is renewing certificate or not.
   * @return String         - pem encoded SCM signed
   *                          certificate.
   */
  String getSCMCertificate(ScmNodeDetailsProto scmNodeDetails,
      String certSignReq, boolean isRenew) throws IOException;

  /**
   * Get SCM signed certificate for given certificate serial id if it exists.
   * Throws exception if it's not found.
   *
   * @param certSerialId    - Certificate serial id.
   * @return String         - pem encoded SCM signed
   *                          certificate with given cert id if it
   *                          exists.
   */
  String getCertificate(String certSerialId) throws IOException;

  /**
   * Get CA certificate.
   *
   * @return String         - pem encoded CA certificate.
   */
  String getCACertificate() throws IOException;

  /**
   * Get list of certificates meet the query criteria.
   *
   * @param type            - node type: OM/SCM/DN.
   * @param startSerialId   - start certificate serial id.
   * @param count           - max number of certificates returned in a batch.
   * @return list of PEM encoded certificate strings.
   */
  List<String> listCertificate(HddsProtos.NodeType type, long startSerialId, int count) throws IOException;

  /**
   * Get Root CA certificate.
   * @throws IOException
   */
  String getRootCACertificate() throws IOException;

  /**
   * Returns all the individual SCM CA's along with Root CA.
   *
   * For example 3 nodes SCM HA cluster, the output will be
   *
   * SCM1 CA
   * SCM2 CA
   * SCM3 CA
   * Root CA
   * @return list of CA's
   *
   * For example on non-HA cluster the output will be SCM CA and Root CA.
   *
   * SCM CA
   * Root CA
   *
   * @throws IOException
   */
  List<String> listCACertificate() throws IOException;

  /**
   * Get SCM signed certificate.
   *
   * @param nodeDetails - Node Details.
   * @param certSignReq - Certificate signing request.
   * @return String      - pem encoded SCM signed
   * certificate.
   */
  String getCertificate(NodeDetailsProto nodeDetails,
      String certSignReq) throws IOException;

  /**
   * Get all root CA certificates known to SCM.
   *
   * @return String     - pem encoded list of root CA certificates
   */
  List<String> getAllRootCaCertificates() throws IOException;

  /**
   * Remove all expired certificates from the SCM metadata store.
   *
   * @return list of the removed certificates
   * @throws IOException
   */
  List<String> removeExpiredCertificates() throws IOException;
}

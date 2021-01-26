/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.cli.cert;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Visibility;
import picocli.CommandLine.Option;

/**
 * This is the handler that process certificate list command.
 */
@Command(
    name = "list",
    description = "List certificates",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ListSubcommand extends ScmCertSubcommand {

  private static final Logger LOG =
      LoggerFactory.getLogger(ListSubcommand.class);

  @Option(names = {"-s", "--start"},
      description = "Certificate serial id to start the iteration",
      defaultValue = "0", showDefaultValue = Visibility.ALWAYS)
  private long startSerialId;

  @Option(names = {"-c", "--count"},
      description = "Maximum number of certificates to list",
      defaultValue = "20", showDefaultValue = Visibility.ALWAYS)
  private int count;

  @Option(names = {"-r", "--role"},
      description = "Filter certificate by the role: om/datanode",
      defaultValue = "datanode", showDefaultValue = Visibility.ALWAYS)
  private String role;

  @Option(names = {"-t", "--type"},
      description = "Filter certificate by the type: valid or revoked",
      defaultValue = "valid", showDefaultValue = Visibility.ALWAYS)
  private String type;
  private static final String OUTPUT_FORMAT = "%-17s %-30s %-30s %-110s";

  private HddsProtos.NodeType parseCertRole(String r) {
    if (r.equalsIgnoreCase("om")) {
      return HddsProtos.NodeType.OM;
    } else if (r.equalsIgnoreCase("scm")) {
      return HddsProtos.NodeType.SCM;
    } else {
      return HddsProtos.NodeType.DATANODE;
    }
  }

  private void printCert(X509Certificate cert) {
    LOG.info(String.format(OUTPUT_FORMAT, cert.getSerialNumber(),
        cert.getNotBefore(), cert.getNotAfter(), cert.getSubjectDN()));
  }

  @Override
  protected void execute(SCMSecurityProtocol client) throws IOException {
    boolean isRevoked = type.equalsIgnoreCase("revoked");
    List<String> certPemList = client.listCertificate(
        parseCertRole(role), startSerialId, count, isRevoked);
    LOG.info("Total {} {} certificates: ", certPemList.size(), type);
    LOG.info(String.format(OUTPUT_FORMAT, "SerialNumber", "Valid From",
        "Expiry", "Subject"));
    for (String certPemStr : certPemList) {
      try {
        X509Certificate cert = CertificateCodec.getX509Certificate(certPemStr);
        printCert(cert);
      } catch (CertificateException ex) {
        LOG.error("Failed to parse certificate.");
      }
    }
  }
}

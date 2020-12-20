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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;

import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * This is the handler that process certificate info command.
 */
@Command(
    name = "info",
    description = "Show detailed information for a specific certificate",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)

class InfoSubcommand extends ScmCertSubcommand {

  private static final Logger LOG =
      LoggerFactory.getLogger(InfoSubcommand.class);

  @Spec
  private CommandSpec spec;

  @Parameters(description = "Serial id of the certificate in decimal.")
  private String serialId;

  @Override
  public void execute(SCMSecurityProtocol client) throws IOException {
    final String certPemStr =
        client.getCertificate(serialId);
    Preconditions.checkNotNull(certPemStr,
        "Certificate can't be found");

    // Print container report info.
    LOG.info("Certificate id: {}", serialId);
    try {
      X509Certificate cert = CertificateCodec.getX509Cert(certPemStr);
      LOG.info(cert.toString());
    } catch (CertificateException ex) {
      LOG.error("Failed to get certificate id " + serialId);
      throw new IOException("Fail to get certificate id " + serialId, ex);
    }
  }
}
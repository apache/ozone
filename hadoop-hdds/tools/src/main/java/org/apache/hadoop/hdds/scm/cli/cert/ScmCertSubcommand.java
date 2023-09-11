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

import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.scm.cli.ScmOption;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.slf4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Base class for admin commands that connect via SCM security client.
 */
public abstract class ScmCertSubcommand implements Callable<Void> {

  @CommandLine.Mixin
  private ScmOption scmOption;

  private static final String OUTPUT_FORMAT = "%-17s %-30s %-30s %-110s %-110s";

  protected void printCertList(Logger log, List<String> pemEncodedCerts) {
    if (pemEncodedCerts.isEmpty()) {
      log.info("No certificates to list");
      return;
    }
    log.info(String.format(OUTPUT_FORMAT, "SerialNumber", "Valid From",
        "Expiry", "Subject", "Issuer"));
    for (String certPemStr : pemEncodedCerts) {
      try {
        X509Certificate cert = CertificateCodec.getX509Certificate(certPemStr);
        printCert(cert, log);
      } catch (CertificateException e) {
        log.error("Failed to parse certificate.", e);
      }
    }
  }

  protected void printCert(X509Certificate cert, Logger log) {
    log.info(String.format(OUTPUT_FORMAT, cert.getSerialNumber(),
        cert.getNotBefore(), cert.getNotAfter(), cert.getSubjectDN(),
        cert.getIssuerDN()));
  }

  protected abstract void execute(SCMSecurityProtocol client)
      throws IOException;

  @Override
  public final Void call() throws Exception {
    SCMSecurityProtocol client = scmOption.createScmSecurityClient();
    execute(client);
    return null;
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.admin.om;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Handler of om roles command.
 */
@CommandLine.Command(
    name = "certificates", aliases = "certs",
    description = "List all CA certificates from OM",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class GetCertificatesSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(names = {"-id", "--service-id"},
      description = "OM Service ID, for HA mode."
  )
  private String omServiceId;

  @CommandLine.Option(
      names = {"-host", "--service-host"},
      description = "Ozone Manager Host, for non-HA mode."
  )
  private String omHost;

  @CommandLine.Option(
      names = {"--show", "-show"},
      required = true
  )
  private boolean action;

  private OzoneManagerProtocol ozoneManagerClient;

  private static final String OUTPUT_FORMAT = "%-17s %-30s %-30s %-110s";

  @Override
  public Void call() throws Exception {
    try {
      ozoneManagerClient =  parent.createOmClient(omServiceId, omHost, false);
      List<X509Certificate> certs = getCACertificates(
          ozoneManagerClient.getServiceInfo());
      for (X509Certificate cert: certs) {
        String output = String.format(OUTPUT_FORMAT, cert.getSerialNumber(),
            cert.getNotBefore(), cert.getNotAfter(), cert.getSubjectDN());
        System.out.println(output);
      }
    } catch (Exception ex) {
      System.out.printf("Error: %s", ex.getMessage());
    } finally {
      if (ozoneManagerClient != null) {
        ozoneManagerClient.close();
      }
    }
    return null;
  }

  private List<X509Certificate> getCACertificates(ServiceInfoEx serviceInfoEx)
      throws IOException {
    return OzoneSecurityUtil.convertToX509(serviceInfoEx.getCaCertPemList());
  }
}

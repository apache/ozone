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

package org.apache.hadoop.hdds.scm.cli.cert;

import static java.lang.System.err;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOException;
import java.math.BigInteger;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.server.JsonUtils;
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
      description = "This option is unused currently, and has no effect on the output.",
      defaultValue = "VALID", showDefaultValue = Visibility.NEVER)
  @Deprecated
  private String type;
  
  @Option(names = { "--json" },
      defaultValue = "false",
      description = "Format output as JSON")
  private boolean json;
  
  private HddsProtos.NodeType parseCertRole(String r) {
    if (r.equalsIgnoreCase("om")) {
      return HddsProtos.NodeType.OM;
    } else if (r.equalsIgnoreCase("scm")) {
      return HddsProtos.NodeType.SCM;
    } else {
      return HddsProtos.NodeType.DATANODE;
    }
  }

  @Override
  protected void execute(SCMSecurityProtocol client) throws IOException {
    HddsProtos.NodeType nodeType = parseCertRole(role);
    List<String> certPemList = client.listCertificate(nodeType, startSerialId, count);
    if (count == certPemList.size()) {
      err.println("The certificate list could be longer than the batch size: "
          + count + ". Please use the \"-c\" option to see more" +
          " certificates.");
    }

    if (json) {
      err.println("Certificate list:(BatchSize=" + count + ", CertCount=" + certPemList.size() + ")");
      List<Certificate> certList = new ArrayList<>();
      for (String certPemStr : certPemList) {
        try {
          X509Certificate cert =
              CertificateCodec.getX509Certificate(certPemStr);
          certList.add(new Certificate(cert));
        } catch (CertificateException ex) {
          err.println("Failed to parse certificate.");
        }
      }
      System.out.println(
          JsonUtils.toJsonStringWithDefaultPrettyPrinter(certList));
      return;
    }

    System.out.printf("Certificate list:(BatchSize=%s, CertCount=%s)%n", count, certPemList.size());
    printCertList(certPemList);
  }

  private static class BigIntJsonSerializer extends JsonSerializer<BigInteger> {
    @Override
    public void serialize(BigInteger value, JsonGenerator jgen,
                          SerializerProvider provider)
        throws IOException {
      jgen.writeNumber(String.format("%d", value));
    }
  }

  private static class Certificate {
    private BigInteger serialNumber;
    private String validFrom;
    private String expiry;
    private Map<String, String> subjectDN = new LinkedHashMap<>();
    private Map<String, String> issuerDN = new LinkedHashMap<>();

    Certificate(X509Certificate cert) {
      serialNumber = cert.getSerialNumber();
      validFrom = cert.getNotBefore().toString();
      expiry = cert.getNotAfter().toString();

      String subject = cert.getSubjectDN().getName();
      parseDnInfo(subject, true);

      String issuer = cert.getIssuerDN().getName();
      parseDnInfo(issuer, false);

    }

    private void parseDnInfo(String dnName, boolean isSubject) {
      String[] dnNameComponents = dnName.split(",");
      if (dnNameComponents.length == 0) {
        err.println("Invalid format of name: " + dnName);
      } else {
        for (String elem : dnNameComponents) {
          String[] components = elem.split("=");
          if (components.length == 2) {
            (isSubject ? subjectDN : issuerDN)
                .put(components[0], components[1]);
          } else {
            err.println("Invalid format of name: " + dnName);
          }
        }
      }
    }

    @JsonSerialize(using = BigIntJsonSerializer.class)
    public BigInteger getSerialNumber() {
      return serialNumber;
    }

    public String getValidFrom() {
      return validFrom;
    }

    public String getExpiry() {
      return expiry;
    }

    public Map<String, String> getSubjectDN() {
      return subjectDN;
    }

    public Map<String, String> getIssuerDN() {
      return issuerDN;
    }
  }
}

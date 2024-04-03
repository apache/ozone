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

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import picocli.CommandLine;

import java.io.IOException;
import java.util.List;

/**
 * This is the handler to clean SCM database from expired certificates.
 */
@CommandLine.Command(
    name = "clean",
    description = "Clean expired certificates from the SCM metadata.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class CleanExpiredCertsSubcommand extends ScmCertSubcommand {

  @Override
  protected void execute(SCMSecurityProtocol client) throws IOException {
    List<String> pemEncodedCerts = client.removeExpiredCertificates();
    System.out.println("List of removed expired certificates:");
    printCertList(pemEncodedCerts);
  }
}

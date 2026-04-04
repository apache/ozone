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

package org.apache.hadoop.ozone.debug.kdiag;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.DebugSubcommand;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

/**
 * Kerberos diagnostic tool for Ozone.
 * Usage:Validates each registered probe serially
 * and prints diagnostic summary.
 * Example: ozone debug kdiag
 */
@CommandLine.Command(name = "kdiag", description = "Diagnose Kerberos configuration issues for Ozone.")
@MetaInfServices(DebugSubcommand.class)
public class OzoneKDiag implements Callable<Void>, DebugSubcommand {
  @Override
  public Void call() throws Exception {
    System.out.println("\n== Ozone Kerberos Diagnostics ==\n");
    List<DiagnosticProbe> probes =
        Arrays.asList(new HostProbe(),
            new EnvironmentProbe(),
            new JvmKerberosProbe(),
            new KerberosConfigProbe(),
            new KinitProbe(),
            new KerberosTicketProbe(),
            new PrincipalMappingProbe(),
            new OzonePrincipalProbe(),
            new KeytabProbe(),
            new SecurityConfigProbe(),
            new AuthorizationProbe(),
            new HttpAuthProbe());
    int pass = 0;
    int warn = 0;
    int fail = 0;
    for (DiagnosticProbe probe : probes) {
      try {
        boolean result = probe.run();
        if (result) {
          pass++;
          System.out.println("[PASS] " + probe.name());
        } else {
          warn++;
          System.out.println("[WARN] " + probe.name());
        }
      } catch (Exception e) {
        fail++;
        System.out.println("[FAIL] " + probe.name() + " : " + e.getMessage());
      }
      System.out.println();
    }
    System.out.println("== Diagnostic Summary ==");
    System.out.println("PASS : " + pass);
    System.out.println("WARN : " + warn);
    System.out.println("FAIL : " + fail);
    return null;
  }
}

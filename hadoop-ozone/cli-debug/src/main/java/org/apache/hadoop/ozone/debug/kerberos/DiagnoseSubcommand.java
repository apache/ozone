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

package org.apache.hadoop.ozone.debug.kerberos;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import picocli.CommandLine;

/**
 * Kerberos diagnostic command for Ozone.
 * Usage:Validates each registered probe serially
 * and prints diagnostic summary.
 * Example: ozone debug kerberos diagnose
 */
@CommandLine.Command(name = "diagnose",
    description = "Diagnose Kerberos configuration issues.")
public class DiagnoseSubcommand extends AbstractSubcommand
    implements Callable<Integer> {
  @Override
  public Integer call() throws Exception {
    out().println("\n== Ozone Kerberos Diagnostics ==\n");
    OzoneConfiguration conf = getOzoneConf();
    List<DiagnosticProbe> probes = Arrays.asList(
        new HostProbe(),
        new EnvironmentProbe(),
        new JvmKerberosProbe(),
        new KerberosConfigProbe(),
        new KinitProbe(),
        new KeytabProbe(),
        new KerberosTicketProbe(),
        new PrincipalMappingProbe(),
        new SecurityConfigProbe(),
        new AuthorizationProbe(),
        new HttpAuthProbe());

    int pass = 0, warn = 0, fail = 0;

    for (DiagnosticProbe probe : probes) {

      out().println("-- " + probe.name() + " --");

      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(
          buffer, true, StandardCharsets.UTF_8.name());
      PrintStream oldOut = System.out;
      PrintStream oldErr = System.err;

      System.setOut(ps);
      System.setErr(ps);

      ProbeResult result;
      try {
        result = probe.test(conf);
      } catch (Throwable t) {
        t.printStackTrace(System.err);
        err().println("ERROR: Probe execution failed: " + t.getMessage());
        result = ProbeResult.FAIL;
      } finally {
        System.setOut(oldOut);
        System.setErr(oldErr);
        ps.close();
      }

      String output = new String(buffer.toByteArray(), StandardCharsets.UTF_8);
      out().print(output);

      switch (result) {
      case FAIL:
        fail++;
        out().println("[FAIL] " + probe.name());
        break;
      case WARN:
        warn++;
        out().println("[WARN] " + probe.name());
        break;
      case PASS:
        pass++;
        out().println("[PASS] " + probe.name());
        break;
      default:
        throw new IllegalStateException("Unknown result: " + result);
      }
      out().println();
    }
    out().println("== Diagnostic Summary ==");
    out().println("PASS : " + pass);
    out().println("WARN : " + warn);
    out().println("FAIL : " + fail);

    return fail > 0 ? 1 : 0;
  }
}

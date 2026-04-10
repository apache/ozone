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

import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.security.authentication.util.KerberosName;
import picocli.CommandLine;

/**
 * Debug command to translate one or more Kerberos principals into local user name.
 * using the configured auth_to_local rules.
 * Example: ozone debug kerberos translate-principal <principal>
 */
@CommandLine.Command(
    name = "translate-principal",
    description = "Translate Kerberos principal(s) using auth_to_local rules."
)
public class TranslatePrincipalSubcommand extends AbstractSubcommand
    implements Callable<Integer> {

  @CommandLine.Parameters(arity = "1..*",
      description = "Kerberos principal(s) to translate"
  )
  private List<String> principals;

  @Override
  public Integer call() throws Exception {

    out().println("\n== Kerberos Principal Translation  ==\n");

    String rules = getOzoneConf().getTrimmed(
        "hadoop.security.auth_to_local", "DEFAULT");
    out().println("auth_to_local rules = " + rules);

    KerberosName.setRules(rules);
    int pass = 0;
    int fail = 0;
    for (String principal : principals) {
      out().println("-- " + principal + " --");
      try {
        KerberosName kerbName = new KerberosName(principal);
        String shortName = kerbName.getShortName();
        out().println(String.format("Principal = %s to " +
            "Local user = %s", principal, shortName));
        pass++;
        out().println("[PASS] " + principal);
      } catch (Exception e) {
        err().println("ERROR: Failed to translate principal "
            + principal + " : " + e.getMessage());
        fail++;
        out().println("[FAIL] " + principal);
      }
      out().println();
    }
    out().println("== Translation Summary ==");
    out().println("PASS : " + pass);
    out().println("FAIL : " + fail);

    return fail > 0 ? 1 : 0;
  }
}

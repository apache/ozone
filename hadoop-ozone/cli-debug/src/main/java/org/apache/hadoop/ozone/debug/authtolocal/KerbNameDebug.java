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

package org.apache.hadoop.ozone.debug.authtolocal;

import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.DebugSubcommand;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

/**
 * Debug command to translate Kerberos principals into local user names
 * using the configured auth_to_local rules.
 *
 * Example:
 *   ozone debug kerbname om/om1@EXAMPLE.COM
 */
@CommandLine.Command(
    name = "kerbname",
    description = "Translate Kerberos principal(s) using auth_to_local rules."
)
@MetaInfServices(DebugSubcommand.class)
public class KerbNameDebug implements Callable<Void>, DebugSubcommand {

  @CommandLine.Parameters(arity = "1..*",
      description = "Kerberos principal(s) to translate"
  )
  private List<String> principals;

  @Override
  public Void call() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    String rules = conf.get("hadoop.security.auth_to_local");
    if (rules != null) {
      KerberosName.setRules(rules);
    }
    System.out.println("-- Kerberos Principal Translation --");
    for (String principal : principals) {
      KerberosName name = new KerberosName(principal);
      System.out.println(
          principal + " -> " + name.getShortName()
      );
    }
    return null;
  }
}

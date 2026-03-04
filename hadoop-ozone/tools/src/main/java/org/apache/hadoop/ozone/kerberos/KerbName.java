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

package org.apache.hadoop.ozone.kerberos;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.security.authentication.util.KerberosName;
/**
 * Command line utility to translate Kerberos principals to local user names
 * using the configured {@code hadoop.security.auth_to_local} rules.
 * <p>Example usage:</p>
 * <pre>
 *   ozone kerbname <principal>
 * </pre>
 *
 */
public class KerbName
{
  public static void main(String[] args) throws Exception {

    if (args.length == 0) {
      System.err.println("Usage: ozone kerbname <principal>");
      return;
    }

    OzoneConfiguration conf = new OzoneConfiguration();
    String rules = conf.get("hadoop.security.auth_to_local");
    if (rules != null) {
      KerberosName.setRules(rules);
    }

    for (String principal : args) {
     KerberosName name = new KerberosName(principal);
     System.out.println("Name: " + name + " to " + name.getShortName());
    }
  }
}

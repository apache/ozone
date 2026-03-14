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

import java.io.File;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

/**
 * Validates existence and readability of Ozone service keytabs.
 *
 * This probe checks the configured keytab files for major Ozone
 * services such as OM, SCM, recon, s3g and DataNode.
 */
public class KeytabProbe implements DiagnosticProbe {

  @Override
  public String name() {
    return "Keytab Validation";
  }

  @Override
  public boolean run() {
    System.out.println("-- Keytab Validation --");
    OzoneConfiguration conf = new OzoneConfiguration();
    checkKeytab(conf.get("ozone.om.kerberos.keytab.file"));
    checkKeytab(conf.get("hdds.scm.kerberos.keytab.file"));
    checkKeytab(conf.get("hdds.datanode.kerberos.keytab.file"));
    checkKeytab(conf.get("ozone.recon.kerberos.keytab.file"));
    checkKeytab(conf.get("ozone.s3g.kerberos.keytab.file"));
    return true;
  }

  /**
   * Check whether the given keytab exists and is readable.
   */
  private void checkKeytab(String path) {

    if (path == null || path.isEmpty()) {
      return;
    }
    File file = new File(path);
    if (!file.exists()) {
      System.out.println("WARNING: keytab missing: " + path);
    } else if (!file.canRead()) {
      System.out.println("WARNING: keytab not readable: " + path);
    } else {
      System.out.println("Keytab OK: " + path);
    }
  }
}

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

import java.io.File;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

/**
 * Validates that kinit is available on the system PATH.
 */
public class KinitProbe extends ConfigProbe {

  @Override
  public String name() {
    return "Kerberos kinit Command";
  }

  @Override
  public ProbeResult test(OzoneConfiguration conf) {

    String path = System.getenv("PATH");

    if (path == null) {
      printValue("PATH", "(unset)");
      error("kinit cannot be resolved because PATH is not set.");
      return ProbeResult.FAIL;
    }

    printValue("PATH", path);
    for (String dir : path.split(":")) {
      File file = new File(dir, "kinit");
      if (file.exists()) {
        if (!file.canExecute()) {
          error("kinit found but not executable: " +
              file.getAbsolutePath());
          return ProbeResult.FAIL;
        }
        printValue("kinit found at ",
            file.getAbsolutePath());
        return ProbeResult.PASS;
      }
    }
    error("kinit not found on PATH");
    return ProbeResult.FAIL;
  }
}

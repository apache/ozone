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

/**
 * Validates that kinit is available on the system PATH.
 */
public class KinitProbe implements DiagnosticProbe {

  @Override
  public String name() {
    return "Kerberos kinit Command";
  }

  @Override
  public boolean run() {
    System.out.println("-- Kerberos kinit Command --");
    String path = System.getenv("PATH");
    if (path == null) {
      System.out.println("PATH = (unset)");
      System.out.println("kinit cannot be resolved");
      return false;
    }
    System.out.println("Executable kinit must be available on PATH");
    System.out.println("PATH = " + path);
    for (String dir : path.split(":")) {
      File candidate = new File(dir, "kinit");
      if (candidate.exists() && candidate.canExecute()) {
        System.out.println("kinit found at " + candidate.getAbsolutePath());
        return true;
      }
    }
    System.out.println("kinit not found on PATH");
    return false;
  }
}

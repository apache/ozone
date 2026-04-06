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
import java.io.FileInputStream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

/**
 * Base class for probes with common helpers.
 */
public abstract class ConfigProbe implements DiagnosticProbe {

  /**
   * Generic key-value printer (used everywhere).
   */
  protected void printValue(String key, String value) {
    System.out.println(key + " = " +
        (value == null ? "(unset)" : value));
  }

  /**
   * Config-specific printer.
   */
  protected void print(OzoneConfiguration conf, String key) {
    printValue(key, conf.getTrimmed(key));
  }

  /**
   * Warning message.
   */
  protected void warn(String msg) {
    System.err.println("WARNING: " + msg);
  }

  /**
   * Error message.
   */
  protected void error(String msg) {
    System.err.println("ERROR: " + msg);
  }

  /**
   *
   * @param file
   * @param description
   * @return
   */
  protected boolean canReadFile(File file, String description) {
    try (FileInputStream fis = new FileInputStream(file)) {
      if (fis.read() == -1) {
        error(description + " is empty or invalid: " + file);
        return false;
      }
      return true;
    } catch (Exception e) {
      error(description + " is not readable: " + file + " (" + e.getMessage() + ")");
      return false;
    }
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.metrics.impl;

import java.io.PrintWriter;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;

/**
 * Helpers for config tests and debugging
 */
class ConfigUtil {

  static void dump(Configuration c) {
    dump(null, c, new PrintWriter(System.out));
  }

  static void dump(String header, Configuration c) {
    dump(header, c, new PrintWriter(System.out));
  }

  static void dump(String header, Configuration c, PrintWriter out) {
    PropertiesConfiguration p = new PropertiesConfiguration();
    p.copy(c);
    if (header != null) {
      out.println(header);
    }
    try { p.write(out); }
    catch (Exception e) {
      throw new RuntimeException("Error saving config", e);
    }
  }

  static void assertEq(Configuration expected, Configuration actual) {
    // Check that the actual config contains all the properties of the expected
    for (Iterator<?> it = expected.getKeys(); it.hasNext();) {
      String key = (String) it.next();
      assertTrue(actual.containsKey(key), "actual should contain "+ key);
      assertEquals(expected.getProperty(key),
          actual.getProperty(key), "value of "+ key);
    }
    // Check that the actual config has no extra properties
    for (Iterator<?> it = actual.getKeys(); it.hasNext();) {
      String key = (String) it.next();
      assertTrue(expected.containsKey(key), "expected should contain "+ key);
    }
  }
}

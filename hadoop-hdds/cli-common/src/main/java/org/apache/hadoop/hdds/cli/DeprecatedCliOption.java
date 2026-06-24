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

package org.apache.hadoop.hdds.cli;

import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Emits warnings when deprecated multi-character short CLI options are used.
 */
public final class DeprecatedCliOption {

  private static final Map<String, String> DEPRECATED_OPTIONS = buildDeprecatedOptions();

  private DeprecatedCliOption() {
    // no instances
  }

  private static Map<String, String> buildDeprecatedOptions() {
    Map<String, String> options = new LinkedHashMap<>();
    options.put("-conf", "--conf");
    options.put("-id", "--service-id");
    options.put("-host", "--service-host");
    options.put("-nodeid", "--nodeid");
    options.put("-hostname", "--node-host-address");
    options.put("-al", "--acls");
    options.put("-ffc", "--filter-by-factor");
    options.put("-fst", "--filter-by-state");
    options.put("-tawt", "--transaction-apply-wait-timeout");
    options.put("-tact", "--transaction-apply-check-interval");
    options.put("-pct", "--prepare-check-interval");
    options.put("-pt", "--prepare-timeout");
    return options;
  }

  /**
   * If {@code arg} is a deprecated option (with or without {@code =value} part),
   * print a warning to stderr and return with the recommended replacement option.
   */
  public static String toNonDeprecated(String arg, PrintWriter err) {
    if (arg == null || arg.isEmpty()) {
      return arg;
    }

    String result = arg;
    String[] parts = arg.split("=", 2);
    String opt = parts[0];
    String optToUse = DEPRECATED_OPTIONS.getOrDefault(opt, opt);

    if (!Objects.equals(opt, optToUse)) {
      warn(err, opt, optToUse);
      result = parts.length == 2
          ? optToUse + '=' + parts[1]
          : optToUse;
    }

    return result;
  }

  private static void warn(PrintWriter err, String deprecated, String replacement) {
    err.printf("WARNING: Option '%s' is deprecated. Use '%s' instead.%n",
        deprecated, replacement);
  }
}

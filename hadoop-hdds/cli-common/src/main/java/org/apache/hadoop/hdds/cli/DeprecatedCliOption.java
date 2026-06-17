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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import picocli.CommandLine;

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

  public static boolean isDeprecated(String name) {
    return DEPRECATED_OPTIONS.containsKey(name);
  }

  public static String[] withoutDeprecated(String[] names) {
    return Arrays.stream(names)
        .filter(name -> !isDeprecated(name))
        .toArray(String[]::new);
  }

  /**
   * Print a warning to stderr for each deprecated option present on the command line.
   */
  public static void warnIfMatched(CommandLine.ParseResult parseResult) {
    if (parseResult == null) {
      return;
    }

    for (CommandLine cli : parseResult.asCommandLineList()) {
      CommandLine.ParseResult subcommandResult = cli.getParseResult();
      if (subcommandResult.matchedOptions().isEmpty()) {
        continue;
      }
      for (Map.Entry<String, String> entry : DEPRECATED_OPTIONS.entrySet()) {
        if (subcommandResult.hasMatchedOption(entry.getKey())) {
          warn(cli.getErr(), entry.getKey(), entry.getValue());
        }
      }
    }
  }

  private static void warn(PrintWriter err, String deprecated, String replacement) {
    err.printf("WARNING: Option '%s' is deprecated. Use '%s' instead.%n",
        deprecated, replacement);
  }
}

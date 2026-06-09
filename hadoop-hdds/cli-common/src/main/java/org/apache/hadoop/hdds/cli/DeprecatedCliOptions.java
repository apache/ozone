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

import java.util.Optional;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;

/**
 * Helpers for deprecated CLI option aliases (camelCase, underscore, etc.).
 */
public final class DeprecatedCliOptions {

  private DeprecatedCliOptions() {
  }

  public static boolean hasMatchedOption(CommandSpec spec, String optionName) {
    if (spec == null) {
      return false;
    }
    CommandLine.ParseResult parseResult = spec.commandLine().getParseResult();
    return parseResult != null && parseResult.hasMatchedOption(optionName);
  }

  public static <T> Optional<T> resolveOptional(Optional<T> canonical,
      Optional<T> deprecated) {
    if (canonical != null && canonical.isPresent()) {
      return canonical;
    }
    return deprecated;
  }

  public static String resolveString(String canonical, String deprecated) {
    if (canonical != null && !canonical.isEmpty()) {
      return canonical;
    }
    return deprecated;
  }

  public static void warnIfDeprecatedOptionalUsed(String deprecatedFlag,
      String canonicalFlag, Optional<?> deprecated, Optional<?> canonical) {
    if (deprecated != null && deprecated.isPresent()
        && (canonical == null || !canonical.isPresent())) {
      printWarning(deprecatedFlag, canonicalFlag);
    }
  }

  public static void warnIfDeprecatedStringUsed(String deprecatedFlag,
      String canonicalFlag, String deprecated, String canonical) {
    if (deprecated != null && !deprecated.isEmpty()
        && (canonical == null || canonical.isEmpty())) {
      printWarning(deprecatedFlag, canonicalFlag);
    }
  }

  public static void warnIfDeprecatedUsedWithoutCanonical(String deprecatedFlag,
      String canonicalFlag, CommandSpec spec, String... canonicalOptionNames) {
    if (spec == null || !hasMatchedOption(spec, deprecatedFlag)) {
      return;
    }
    for (String canonicalOptionName : canonicalOptionNames) {
      if (hasMatchedOption(spec, canonicalOptionName)) {
        return;
      }
    }
    printWarning(deprecatedFlag, canonicalFlag);
  }

  private static void printWarning(String deprecatedFlag, String canonicalFlag) {
    System.err.println("warning: " + deprecatedFlag + " is deprecated, use "
        + canonicalFlag + " instead.");
  }
}

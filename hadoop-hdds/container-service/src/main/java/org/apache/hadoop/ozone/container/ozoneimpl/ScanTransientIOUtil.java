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

package org.apache.hadoop.ozone.container.ozoneimpl;

import java.nio.file.FileSystemException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Set;
import org.apache.hadoop.ozone.container.common.interfaces.ScanResult;

/**
 * Utility to catch transient scan failures (typically related to file-descriptor exhaustion)
 * that should not be treated as container data corruption.
 */
public final class ScanTransientIOUtil {

  private static final int MAX_CAUSE_CHAIN_DEPTH = 64;

  private static final String TOO_MANY_OPEN_FILES = "too many open files";

  private ScanTransientIOUtil() {
  }

  /**
   * Returns true when every scan error is related to file-descriptor exhaustion.
   * Each error's exception chain is checked via {@link #isTooManyOpenFiles(Throwable)}.
   */
  public static boolean scanErrorsAreOnlyTooManyOpenFiles(ScanResult scanResult) {
    if (!scanResult.hasErrors()) {
      return false;
    }
    return scanResult.getErrors().stream()
        .allMatch(scanError -> isTooManyOpenFiles(scanError.getException()));
  }

  public static boolean isTooManyOpenFiles(Throwable throwable) {
    if (throwable == null) {
      return false;
    }
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    int depth = 0;
    for (Throwable cause = throwable;
        cause != null && depth < MAX_CAUSE_CHAIN_DEPTH;
        cause = cause.getCause(), depth++) {
      if (!visited.add(cause)) {
        break;
      }
      if (matchesTooManyOpenFiles(cause)) {
        return true;
      }
    }
    return false;
  }

  private static boolean matchesTooManyOpenFiles(Throwable throwable) {
    if (throwable instanceof FileSystemException) {
      String reason = ((FileSystemException) throwable).getReason();
      if (reason != null && containsTooManyOpenFiles(reason)) {
        return true;
      }
    }
    String message = throwable.getMessage();
    return message != null && containsTooManyOpenFiles(message);
  }

  private static boolean containsTooManyOpenFiles(String text) {
    return text.toLowerCase(Locale.ROOT).contains(TOO_MANY_OPEN_FILES);
  }
}

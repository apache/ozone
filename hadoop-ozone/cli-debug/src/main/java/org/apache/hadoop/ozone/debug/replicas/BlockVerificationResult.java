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

package org.apache.hadoop.ozone.debug.replicas;

import java.util.Collections;
import java.util.List;

/**
 * Json structure for replicas to pass through each check and give output.
 */
public class BlockVerificationResult {

  private final boolean completed;
  private final boolean pass;
  private final List<String> failures;

  public BlockVerificationResult(boolean completed, boolean pass, List<String> failures) {
    this.completed = completed;
    this.pass = pass;
    this.failures = failures;
  }

  public static BlockVerificationResult pass() {
    return new BlockVerificationResult(true, true, Collections.emptyList());
  }

  public static BlockVerificationResult failCheck(String message) {
    return new BlockVerificationResult(true, false, Collections.singletonList(message));
  }

  public static BlockVerificationResult failIncomplete(String message) {
    return new BlockVerificationResult(false, false, Collections.singletonList(message));
  }

  public boolean isCompleted() {
    return completed;
  }

  public boolean passed() {
    return pass;
  }

  public List<String> getFailures() {
    return failures;
  }

}

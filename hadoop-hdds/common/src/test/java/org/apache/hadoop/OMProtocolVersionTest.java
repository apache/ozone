/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop;

import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;

import static org.apache.hadoop.ozone.OMProtocolVersion.OM_LATEST_VERSION;

class OMProtocolVersionTest {
  private enum ValidateOmVersionTestCases {
    NULL_EXPECTED_NULL_OM(
        null, // Expected version
        null, // OM Version
        true), // Should validation pass
    EMPTY_EXPECTED_NULL_OM(
        "",
        null,
        true),
    EMPTY_EXPECTED_EMPTY_OM(
        "",
        "",
        true),
    NULL_EXPECTED_EMPTY_OM(
        null,
        "",
        true),
    OM_EXPECTED_LATEST_OM(
        "1.0.0",
        OM_LATEST_VERSION,
        true),
    OM_EXPECTED_NEWER_OM(
        "1.1.0",
        "1.11.0",
        true),
    NEWER_EXPECTED_OLD_OM(
        "1.20.0",
        "1.19.0",
        false),
    NULL_EXPECTED_OM(
        null,
        OM_LATEST_VERSION,
        true);
    private static final List<ValidateOmVersionTestCases>
        TEST_CASES = new LinkedList<>();

    static {
      for (ValidateOmVersionTestCases t : values()) {
        TEST_CASES.add(t);
      }
    }
    private final String expectedVersion;
    private final String omVersion;
    private final boolean validation;
    ValidateOmVersionTestCases(String expectedVersion,
                               String omVersion,
                               boolean validation) {
      this.expectedVersion = expectedVersion;
      this.omVersion = omVersion;
      this.validation = validation;
    }

  }
  @Test
  void isOMCompatible() {
  }
}
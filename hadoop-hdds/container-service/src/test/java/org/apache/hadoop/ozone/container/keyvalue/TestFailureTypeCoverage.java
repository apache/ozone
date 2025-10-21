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

package org.apache.hadoop.ozone.container.keyvalue;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.Set;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScanError;
import org.junit.jupiter.api.Test;

/**
 * Validates test coverage for all {@link ContainerScanError.FailureType} values.
 */
public class TestFailureTypeCoverage {

  @Test
  public void testEveryFailureTypeIsTestedOrExcluded() {
    Set<ContainerScanError.FailureType> testedFailureTypes = EnumSet.noneOf(ContainerScanError.FailureType.class);

    for (TestContainerCorruptions types : TestContainerCorruptions.values()) {
      ContainerScanError.FailureType failureType = types.getExpectedResult();
      assertNotNull(failureType);
      testedFailureTypes.add(failureType);
    }

    Set<ContainerScanError.FailureType> allFailureTypes = EnumSet.allOf(ContainerScanError.FailureType.class);
    Set<ContainerScanError.FailureType> failureTypesMissingTests = EnumSet.copyOf(allFailureTypes);

    //Remove types that are tested
    failureTypesMissingTests.removeAll(testedFailureTypes);
    //Remove explicitly excluded types
    failureTypesMissingTests.removeAll(TestContainerCorruptions.getExcludedFailureTypes());

    assertTrue(failureTypesMissingTests.isEmpty(),
        "The following FailureType values do not have corresponding test corruptions in "
            + "TestContainerCorruptions: " + failureTypesMissingTests + ". Either add test cases for these "
            + "failure types or exclude them with an explanation (via getExcludedFailureTypes()).");
  }
}


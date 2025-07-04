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

import java.util.Collections;
import java.util.List;
import org.apache.hadoop.ozone.container.common.interfaces.ScanResult;

/**
 * Represents the result of a container metadata scan.
 * A metadata scan only checks the existence of container metadata files and the checksum of the .container file.
 * It does not check the data in the container and therefore will not generate a ContainerMerkleTree.
 */
public class MetadataScanResult implements ScanResult {

  private final List<ContainerScanError> errors;
  private final boolean deleted;
  // Results are immutable. Intern the common cases.
  private static final MetadataScanResult HEALTHY_RESULT = new MetadataScanResult(Collections.emptyList(), false);
  private static final MetadataScanResult DELETED = new MetadataScanResult(Collections.emptyList(), true);

  protected MetadataScanResult(List<ContainerScanError> errors, boolean deleted) {
    this.errors = errors;
    this.deleted = deleted;
  }

  /**
   * Constructs a metadata scan result whose health will be determined based on the presence of errors.
   */
  public static MetadataScanResult fromErrors(List<ContainerScanError> errors) {
    if (errors.isEmpty()) {
      return HEALTHY_RESULT;
    } else {
      return new MetadataScanResult(errors, false);
    }
  }

  /**
   * Constructs a metadata scan result representing a container that was deleted during the scan.
   */
  public static MetadataScanResult deleted() {
    return DELETED;
  }

  @Override
  public boolean isDeleted() {
    return deleted;
  }

  @Override
  public boolean hasErrors() {
    return !errors.isEmpty();
  }

  @Override
  public List<ContainerScanError> getErrors() {
    return errors;
  }

  /**
   * @return A string representation of the first error in this result, or a string indicating the result is healthy.
   */
  @Override
  public String toString() {
    if (deleted) {
      return "Container was deleted";
    } else if (errors.isEmpty()) {
      return "Container has 0 errors";
    } else if (errors.size() == 1) {
      return "Container has 1 error: " + errors.get(0);
    } else {
      return "Container has " + errors.size() + " errors. The first error is: " + errors.get(0);
    }
  }
}

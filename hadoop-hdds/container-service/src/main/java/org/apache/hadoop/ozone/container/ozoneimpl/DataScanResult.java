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

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeWriter;

/**
 * Represents the result of a container data scan.
 * A container data scan will do a full metadata check, and check the contents of all block data within the container.
 * The result will contain all the errors seen while scanning the container, and a ContainerMerkleTree representing
 * the data that the scan saw on the disk when it ran.
 */
public final class DataScanResult extends MetadataScanResult {

  private final ContainerMerkleTreeWriter dataTree;
  // Only deleted results can be interned. Healthy results will still have different trees.
  private static final DataScanResult DELETED = new DataScanResult(Collections.emptyList(),
      new ContainerMerkleTreeWriter(), true);

  private DataScanResult(List<ContainerScanError> errors, ContainerMerkleTreeWriter dataTree, boolean deleted) {
    super(errors, deleted);
    this.dataTree = dataTree;
  }

  /**
   * Constructs an unhealthy data scan result which was aborted before scanning any data due to a metadata error.
   * This data scan result will have an empty data tree with a zero checksum to indicate that no data was scanned.
   */
  public static DataScanResult unhealthyMetadata(MetadataScanResult result) {
    Preconditions.checkArgument(result.hasErrors());
    return new DataScanResult(result.getErrors(), new ContainerMerkleTreeWriter(), false);
  }

  /**
   * Constructs a data scan result representing a container that was deleted during the scan.
   */
  public static DataScanResult deleted() {
    return DELETED;
  }

  /**
   * Constructs a data scan result whose health will be determined based on the presence of errors.
   */
  public static DataScanResult fromErrors(List<ContainerScanError> errors, ContainerMerkleTreeWriter dataTree) {
    return new DataScanResult(errors, dataTree, false);
  }

  public ContainerMerkleTreeWriter getDataTree() {
    return dataTree;
  }
}

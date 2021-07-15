/*
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

package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.ozone.recon.ReconConstants;

import java.util.Arrays;

/**
 * HTTP Response wrapped for a file size distribution request.
 * 'dist': the array that stores the file size distribution for all keys
 * under the request path.
 * 'pathNotFound': invalid path request.
 * 'typeNA': the path exists, but refers to a namespace type (key) that are not
 * applicable to file size distribution request.
 */
public class FileSizeDistributionResponse {

  @JsonProperty("dist")
  private int[] fileSizeDist;
  @JsonProperty("pathNotFound")
  private boolean pathNotFound;
  @JsonProperty("typeNA")
  private boolean namespaceNotApplicable;

  public FileSizeDistributionResponse() {
    this.pathNotFound = false;
    this.namespaceNotApplicable = false;
    this.fileSizeDist = null;
  }

  public boolean isPathNotFound() {
    return pathNotFound;
  }

  public int[] getFileSizeDist() {
    return Arrays.copyOf(this.fileSizeDist, ReconConstants.NUM_OF_BINS);
  }

  public boolean isNamespaceNotApplicable() {
    return namespaceNotApplicable;
  }

  public void setPathNotFound(boolean pathNotFound) {
    this.pathNotFound = pathNotFound;
  }

  public void setFileSizeDist(int[] fileSizeDist) {
    this.fileSizeDist = Arrays.copyOf(fileSizeDist, ReconConstants.NUM_OF_BINS);
  }

  public void setNamespaceNotApplicable(boolean namespaceNotApplicable) {
    this.namespaceNotApplicable = namespaceNotApplicable;
  }
}

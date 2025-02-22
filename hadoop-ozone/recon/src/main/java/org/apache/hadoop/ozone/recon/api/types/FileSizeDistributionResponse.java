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

package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import org.apache.hadoop.ozone.recon.ReconConstants;

/**
 * HTTP Response wrapped for a file size distribution request.
 */
public class FileSizeDistributionResponse {
  /**
   * The array that stores the file size distribution for all keys
   * under the request path.
   */
  @JsonProperty("dist")
  private int[] fileSizeDist;

  /** Path status. */
  @JsonProperty("status")
  private ResponseStatus status;

  public FileSizeDistributionResponse() {
    this.status = ResponseStatus.OK;
    this.fileSizeDist = new int[ReconConstants.NUM_OF_FILE_SIZE_BINS];
  }

  public ResponseStatus getStatus() {
    return status;
  }

  public int[] getFileSizeDist() {
    return Arrays.copyOf(this.fileSizeDist,
        ReconConstants.NUM_OF_FILE_SIZE_BINS);
  }

  public void setStatus(ResponseStatus status) {
    this.status = status;
  }

  public void setFileSizeDist(int[] fileSizeDist) {
    this.fileSizeDist =
        Arrays.copyOf(fileSizeDist, ReconConstants.NUM_OF_FILE_SIZE_BINS);
  }
}

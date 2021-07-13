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

import org.apache.hadoop.ozone.recon.ReconConstants;

import java.util.Arrays;

/**
 * Class to encapsulate namespace metadata summaries from OM.
 */

public class NSSummary {
  private int numOfFiles;
  private int sizeOfFiles;
  private int[] fileSizeBucket;

  public NSSummary() {
    this.numOfFiles = 0;
    this.sizeOfFiles = 0;
    this.fileSizeBucket = new int[ReconConstants.NUM_OF_BINS];
  }

  public NSSummary(int numOfFiles, int sizeOfFiles, int[] bucket) {
    this.numOfFiles = numOfFiles;
    this.sizeOfFiles = sizeOfFiles;
    setFileSizeBucket(bucket);
  }

  public int getNumOfFiles() {
    return numOfFiles;
  }

  public int getSizeOfFiles() {
    return sizeOfFiles;
  }

  public int[] getFileSizeBucket() {
    return Arrays.copyOf(this.fileSizeBucket, ReconConstants.NUM_OF_BINS);
  }

  public void setNumOfFiles(int numOfFiles) {
    this.numOfFiles = numOfFiles;
  }

  public void setSizeOfFiles(int sizeOfFiles) {
    this.sizeOfFiles = sizeOfFiles;
  }

  public void setFileSizeBucket(int[] fileSizeBucket) {
    this.fileSizeBucket = Arrays.copyOf(
            fileSizeBucket, ReconConstants.NUM_OF_BINS);
  }
}

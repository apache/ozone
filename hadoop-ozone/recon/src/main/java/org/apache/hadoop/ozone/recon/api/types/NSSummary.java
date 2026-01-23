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

import static org.apache.hadoop.ozone.om.helpers.OzoneFSUtils.removeTrailingSlashIfNeeded;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.ozone.recon.ReconConstants;

/**
 * Class to encapsulate namespace metadata summaries from OM.
 * 
 * IMPORTANT: As of the materialized optimization, sizeOfFiles and numOfFiles 
 * now represent TOTAL values (including this directory and ALL subdirectories)
 * rather than just direct files in this directory, for O(1) disk usage queries.
 */

public class NSSummary {
  // IMPORTANT: These fields now contain TOTAL values (this directory + all subdirectories)
  // for performance optimization, not just direct files in this directory
  private int numOfFiles;
  private long sizeOfFiles;
  private long replicatedSizeOfFiles;
  private int[] fileSizeBucket;
  private Set<Long> childDir;
  private String dirName;
  private long parentId = 0;

  public NSSummary() {
    this(0, 0L, 0L, new int[ReconConstants.NUM_OF_FILE_SIZE_BINS],
        new HashSet<>(), "", 0);
  }

  public NSSummary(int numOfFiles,
                   long sizeOfFiles,
                   long replicatedSizeOfFiles,
                   int[] bucket,
                   Set<Long> childDir,
                   String dirName,
                   long parentId) {
    this.numOfFiles = numOfFiles;
    this.sizeOfFiles = sizeOfFiles;
    this.replicatedSizeOfFiles = replicatedSizeOfFiles;
    setFileSizeBucket(bucket);
    this.childDir = childDir;
    this.dirName = dirName;
    this.parentId = parentId;
  }

  /**
   * @return Total number of files in this directory and ALL subdirectories
   */
  public int getNumOfFiles() {
    return numOfFiles;
  }

  /**
   * @return Total size of files in this directory and ALL subdirectories
   */
  public long getSizeOfFiles() {
    return sizeOfFiles;
  }

  public long getReplicatedSizeOfFiles() {
    return replicatedSizeOfFiles;
  }

  public int[] getFileSizeBucket() {
    return Arrays.copyOf(fileSizeBucket, ReconConstants.NUM_OF_FILE_SIZE_BINS);
  }

  public Set<Long> getChildDir() {
    return childDir;
  }

  public String getDirName() {
    return dirName;
  }

  /**
   * @param numOfFiles Total number of files in this directory and ALL subdirectories
   */
  public void setNumOfFiles(int numOfFiles) {
    this.numOfFiles = numOfFiles;
  }

  /**
   * @param sizeOfFiles Total size of files in this directory and ALL subdirectories
   */
  public void setSizeOfFiles(long sizeOfFiles) {
    this.sizeOfFiles = sizeOfFiles;
  }

  public void setReplicatedSizeOfFiles(long replicatedSizeOfFiles) {
    this.replicatedSizeOfFiles = replicatedSizeOfFiles;
  }

  public void setFileSizeBucket(int[] fileSizeBucket) {
    this.fileSizeBucket = Arrays.copyOf(fileSizeBucket,
            ReconConstants.NUM_OF_FILE_SIZE_BINS);
  }

  public void setChildDir(Set<Long> childDir) {
    this.childDir = childDir;
  }

  public void setDirName(String dirName) {
    this.dirName = removeTrailingSlashIfNeeded(dirName);
  }

  public void addChildDir(long childId) {
    if (this.childDir.contains(childId)) {
      return;
    }
    this.childDir.add(childId);
  }

  public void removeChildDir(long childId) {
    if (this.childDir.contains(childId)) {
      this.childDir.remove(childId);
    }
  }

  public long getParentId() {
    return parentId;
  }

  public void setParentId(long parentId) {
    this.parentId = parentId;
  }

  @Override
  public String toString() {
    return "NSSummary{dirName='" + dirName + '\'' +
        ", parentId=" + parentId +
        ", childDir=" + childDir +
        ", numOfFiles=" + numOfFiles +
        ", sizeOfFiles=" + sizeOfFiles +
        ", replicatedSizeOfFiles=" + replicatedSizeOfFiles +
        ", fileSizeBucket=" + Arrays.toString(fileSizeBucket) +
        '}';
  }
}

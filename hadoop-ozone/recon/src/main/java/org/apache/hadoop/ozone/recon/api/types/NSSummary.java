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
 */

public class NSSummary {
  private int numOfFiles;
  private long sizeOfFiles;
  private int numOfDeletedFiles;  // New field
  private long sizeOfDeletedFiles; // New field
  private int numOfDeletedDirs;   // New field
  private long sizeOfDeletedDirs; // New field
  private int[] fileSizeBucket;
  private Set<Long> childDir;
  private Set<Long> deletedChildDir; // New field to track deleted child directories
  private String dirName;
  private long parentId = 0;

  public NSSummary() {
    this(0, 0L, 0, 0L, 0, 0L, new int[ReconConstants.NUM_OF_FILE_SIZE_BINS],
        new HashSet<>(), new HashSet<>(), "", 0);
  }

  public NSSummary(int numOfFiles,
                   long sizeOfFiles,
                   int numOfDeletedFiles,
                   long sizeOfDeletedFiles,
                   int numOfDeletedDirs,
                   long sizeOfDeletedDirs,
                   int[] bucket,
                   Set<Long> childDir,
                   Set<Long> deletedChildDir,
                   String dirName,
                   long parentId) {
    this.numOfFiles = numOfFiles;
    this.sizeOfFiles = sizeOfFiles;
    this.numOfDeletedFiles = numOfDeletedFiles;
    this.sizeOfDeletedFiles = sizeOfDeletedFiles;
    this.numOfDeletedDirs = numOfDeletedDirs;
    this.sizeOfDeletedDirs = sizeOfDeletedDirs;
    setFileSizeBucket(bucket);
    this.childDir = childDir;
    this.deletedChildDir = deletedChildDir;
    this.dirName = dirName;
    this.parentId = parentId;
  }

  public int getNumOfFiles() {
    return numOfFiles;
  }

  public long getSizeOfFiles() {
    return sizeOfFiles;
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

  public void setNumOfFiles(int numOfFiles) {
    this.numOfFiles = numOfFiles;
  }

  public void setSizeOfFiles(long sizeOfFiles) {
    this.sizeOfFiles = sizeOfFiles;
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

  public int getNumOfDeletedFiles() {
    return numOfDeletedFiles;
  }

  public void setNumOfDeletedFiles(int numOfDeletedFiles) {
    this.numOfDeletedFiles = numOfDeletedFiles;
  }

  public long getSizeOfDeletedFiles() {
    return sizeOfDeletedFiles;
  }

  public void setSizeOfDeletedFiles(long sizeOfDeletedFiles) {
    this.sizeOfDeletedFiles = sizeOfDeletedFiles;
  }

  public int getNumOfDeletedDirs() {
    return numOfDeletedDirs;
  }

  public void setNumOfDeletedDirs(int numOfDeletedDirs) {
    this.numOfDeletedDirs = numOfDeletedDirs;
  }

  public long getSizeOfDeletedDirs() {
    return sizeOfDeletedDirs;
  }

  public void setSizeOfDeletedDirs(long sizeOfDeletedDirs) {
    this.sizeOfDeletedDirs = sizeOfDeletedDirs;
  }

  public Set<Long> getDeletedChildDir() {
    return deletedChildDir;
  }

  public void setDeletedChildDir(Set<Long> deletedChildDir) {
    this.deletedChildDir = deletedChildDir;
  }

  public void addDeletedChildDir(long childId) {
    if (this.deletedChildDir.contains(childId)) {
      return;
    }
    this.deletedChildDir.add(childId);
  }

  public void removeDeletedChildDir(long childId) {
    if (this.deletedChildDir.contains(childId)) {
      this.deletedChildDir.remove(childId);
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
        ", deletedChildDir=" + deletedChildDir +
        ", numOfFiles=" + numOfFiles +
        ", sizeOfFiles=" + sizeOfFiles +
        ", numOfDeletedFiles=" + numOfDeletedFiles +
        ", sizeOfDeletedFiles=" + sizeOfDeletedFiles +
        ", numOfDeletedDirs=" + numOfDeletedDirs +
        ", sizeOfDeletedDirs=" + sizeOfDeletedDirs +
        ", fileSizeBucket=" + Arrays.toString(fileSizeBucket) +
        '}';
  }
}

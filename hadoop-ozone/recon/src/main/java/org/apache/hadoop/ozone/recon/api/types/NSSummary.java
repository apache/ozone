package org.apache.hadoop.ozone.recon.api.types;

import java.util.Arrays;

/**
 * Class to encapsulate namespace metadata summaries from OM
 */

public class NSSummary {
  private int numOfFiles;
  private int sizeOfFiles;
  private int[] fileSizeBucket;

  public NSSummary() {
    this.numOfFiles = 0;
    this.sizeOfFiles = 0;
    // < 1KB, 1KB - 512KB, 513KB - 1024KB, 1MB - 10MB, 10MB - 100MB,
    // 100MB - 1000MB, 1GB - 10GB, 10GB - 100GB, 100GB - 1000GB, > 1TB
    // TODO: can we generate a more reasonable bucket
    // Should this file size distribution be fixed or dynamic based on
    // the actual situation?
    this.fileSizeBucket = new int[10];
  }

  public int getNumOfFiles() {
    return numOfFiles;
  }

  public int getSizeOfFiles() {
    return sizeOfFiles;
  }

  public int[] getFileSizeBucket() {
    return fileSizeBucket;
  }

  public void setNumOfFiles(int numOfFiles) {
    this.numOfFiles = numOfFiles;
  }

  public void setSizeOfFiles(int sizeOfFiles) {
    this.sizeOfFiles = sizeOfFiles;
  }

  public void setFileSizeBucket(int[] fileSizeBucket) {
    this.fileSizeBucket = Arrays.copyOf(fileSizeBucket, 10);
  }
}

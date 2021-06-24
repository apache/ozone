package org.apache.hadoop.ozone.recon.api.types;

import org.apache.hadoop.ozone.recon.ReconConstants;

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
    // TODO: I read the min is 1024(2^10), max is 1PB(2^50), so the number of buckets should be 40?
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
    return fileSizeBucket;
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

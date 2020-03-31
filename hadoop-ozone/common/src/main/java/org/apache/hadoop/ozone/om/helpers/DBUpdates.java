package org.apache.hadoop.ozone.om.helpers;

import java.util.ArrayList;
import java.util.List;

public class DBUpdates {

  private List<byte[]> dataList = new ArrayList<>();

  private long currentSequenceNumber = -1;

  public DBUpdates() {
    this.dataList = new ArrayList<>();
  }

  public DBUpdates(List<byte[]> data) {
    this.dataList = new ArrayList<>(data);
  }

  public void addWriteBatch(byte[] data, long sequenceNumber) {
    dataList.add(data);
    if (currentSequenceNumber < sequenceNumber) {
      currentSequenceNumber = sequenceNumber;
    }
  }

  public List<byte[]> getData() {
    return dataList;
  }

  public void setCurrentSequenceNumber(long sequenceNumber) {
    this.currentSequenceNumber = sequenceNumber;
  }

  public long getCurrentSequenceNumber() {
    return currentSequenceNumber;
  }
}

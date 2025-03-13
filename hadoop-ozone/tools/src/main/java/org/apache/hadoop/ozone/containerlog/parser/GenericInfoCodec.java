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

package org.apache.hadoop.ozone.containerlog.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.StringCodec;

/**
 * Generic information codec for the 2 tables.
 */

public class GenericInfoCodec<T> implements Codec<List<T>> {

  private final StringCodec stringCodec = StringCodec.get();
  private final LongCodec longCodec = LongCodec.get();

  @Override
  public Class<List<T>> getTypeClass() {

    return (Class<List<T>>) (Class<?>) List.class;
  }

  @Override
  public byte[] toPersistedFormat(List<T> object) throws IOException {
    List<byte[]> serializedItems = new ArrayList<>();

    for (T item : object) {
      if (item instanceof ContainerInfo) {
        serializedItems.add(serializeContainerInfo((ContainerInfo) item));
      } else if (item instanceof DatanodeContainerInfo) {
        serializedItems.add(serializeDatanodeContainerInfo((DatanodeContainerInfo) item));
      } else {
        throw new IllegalArgumentException("Unsupported object type");
      }

    }
    return combineByteArrays(serializedItems);
  }


  @Override
  public List<T> fromPersistedFormat(byte[] rawData) throws IOException {

    List<T> objectList = new ArrayList<>();

    if (rawData.length > 0) {
      byte flag = rawData[0];

      if (flag == 1) {
        List<ContainerInfo> containerInfoList = deserializeContainerInfo(rawData);
        objectList.addAll((List<T>) containerInfoList);
      } else if (flag == 2) {
        List<DatanodeContainerInfo> datanodeContainerInfoList = deserializeDatanodeContainerInfo(rawData);
        objectList.addAll((List<T>) datanodeContainerInfoList);

      } else {
        throw new IOException("Unsupported object type flag in data");
      }
    }
    return objectList;

  }


  @Override
  public List<T> copyObject(List<T> object) {
    List<T> copy = new ArrayList<>();
    for (T item : object) {
      if (item instanceof ContainerInfo) {
        ContainerInfo containerInfo = (ContainerInfo) item;
        copy.add((T) new ContainerInfo(containerInfo.getContainerFinalState(), containerInfo.getDatanodeId(),
            containerInfo.getContainerFinalBCSID()));
      } else if (item instanceof DatanodeContainerInfo) {
        DatanodeContainerInfo datanodeContainerInfo = (DatanodeContainerInfo) item;
        copy.add((T) new DatanodeContainerInfo(datanodeContainerInfo.getTimestamp(), datanodeContainerInfo.getState(),
            datanodeContainerInfo.getBcsid(), datanodeContainerInfo.getErrorMessage()));
      }
    }
    return copy;
  }

  private byte[] serializeContainerInfo(ContainerInfo containerInfo) throws IOException {
    byte[] containerFinalStateBytes = stringCodec.toPersistedFormat(containerInfo.getContainerFinalState());
    byte[] datanodeIdBytes = longCodec.toPersistedFormat(containerInfo.getDatanodeId());
    byte[] containerFinalBCSIDBytes = longCodec.toPersistedFormat(containerInfo.getContainerFinalBCSID());

    byte[] lengthByte = new byte[]{(byte) containerFinalStateBytes.length};

    byte[] flag = new byte[]{(byte) 1};

    return concatenateByteArrays(flag, lengthByte, containerFinalStateBytes, datanodeIdBytes, containerFinalBCSIDBytes);
  }


  private List<ContainerInfo> deserializeContainerInfo(byte[] rawData) throws IOException {

    List<ContainerInfo> containerInfoList = new ArrayList<>();
    int offset = 0;

    while (offset < rawData.length) {
      if (rawData[offset] != 1) {
        throw new IOException("Expected flag byte '1' for ContainerInfo, but found " + rawData[offset]);
      }
      offset++;
      if (offset + 1 > rawData.length) {
        throw new IOException("Not enough data for stateLength");
      }
      int stateLength = rawData[offset];
      offset += 1;

      if (offset + stateLength > rawData.length) {
        throw new IOException("Not enough data for containerFinalState");
      }

      String containerFinalState = stringCodec.fromPersistedFormat(sliceArray(rawData, offset, stateLength));
      offset += stateLength;

      if (offset + Long.BYTES > rawData.length) {
        throw new IOException("Not enough data for datanodeId");
      }
      long datanodeId = longCodec.fromPersistedFormat(sliceArray(rawData, offset, Long.BYTES));
      offset += Long.BYTES;

      if (offset + Long.BYTES > rawData.length) {
        throw new IOException("Not enough data for containerFinalBCSID");
      }
      long containerFinalBCSID = longCodec.fromPersistedFormat(sliceArray(rawData, offset, Long.BYTES));
      offset += Long.BYTES;

      containerInfoList.add(new ContainerInfo(containerFinalState, datanodeId, containerFinalBCSID));
    }

    return containerInfoList;
  }

  public byte[] serializeDatanodeContainerInfo(DatanodeContainerInfo datanodeContainerInfo) throws IOException {

    byte[] containerStateTimestampBytes = stringCodec.toPersistedFormat(datanodeContainerInfo.getTimestamp());
    byte[] stateBytes = stringCodec.toPersistedFormat(datanodeContainerInfo.getState());
    byte[] containerBCSIDBytes = longCodec.toPersistedFormat(datanodeContainerInfo.getBcsid());
    byte[] errorMessageBytes = datanodeContainerInfo.getErrorMessage() != null ?
        stringCodec.toPersistedFormat(datanodeContainerInfo.getErrorMessage()) : new byte[0];

    byte[] containerStateTimestampLengthByte = new byte[]{(byte) containerStateTimestampBytes.length};
    byte[] stateLengthByte = new byte[]{(byte) stateBytes.length};
    byte[] errorMessageLengthByte = new byte[]{(byte) errorMessageBytes.length};


    byte[] flag = new byte[]{(byte) 2};

    byte[] result;

    result = concatenateByteArrays(flag, containerStateTimestampLengthByte, containerStateTimestampBytes,
        stateLengthByte, stateBytes, containerBCSIDBytes, errorMessageLengthByte, errorMessageBytes);

    return result;
  }


  public List<DatanodeContainerInfo> deserializeDatanodeContainerInfo(byte[] rawData) throws IOException {

    List<DatanodeContainerInfo> datanodeContainerInfoList = new ArrayList<>();
    int offset = 0;

    while (offset < rawData.length) {
      if (rawData[offset] != 2) {
        throw new IOException(
            "Expected flag byte '2' for DatanodeContainerInfo, but found " + rawData[offset]
        );
      }

      offset++;
      if (offset + 1 > rawData.length) {
        throw new IOException("Not enough data for containerStateTimestamp length");
      }
      int containerStateTimestampLength = rawData[offset];
      offset += 1;

      if (offset + containerStateTimestampLength > rawData.length) {
        throw new IOException("Not enough data for containerStateTimestamp");
      }
      String containerStateTimestamp =
          stringCodec.fromPersistedFormat(sliceArray(rawData, offset, containerStateTimestampLength));
      offset += containerStateTimestampLength;

      if (offset + 1 > rawData.length) {
        throw new IOException("Not enough data for state length");
      }
      int stateLength = rawData[offset];
      offset += 1;

      if (offset + stateLength > rawData.length) {
        throw new IOException("Not enough data for state");
      }
      String state = stringCodec.fromPersistedFormat(sliceArray(rawData, offset, stateLength));
      offset += stateLength;

      if (offset + Long.BYTES > rawData.length) {
        throw new IOException("Not enough data for containerBCSID");
      }
      long containerBCSID = longCodec.fromPersistedFormat(sliceArray(rawData, offset, Long.BYTES));
      offset += Long.BYTES;

      String errorMessage = null;
      if (offset < rawData.length) {
        if (offset + 1 > rawData.length) {
          throw new IOException("Not enough data for errorMessage length");
        }
        int errorMessageLength = rawData[offset] & 0xFF;
        offset += 1;

        if (offset + errorMessageLength > rawData.length) {
          throw new IOException("Not enough data for errorMessage");
        }
        errorMessage = stringCodec.fromPersistedFormat(sliceArray(rawData, offset, errorMessageLength));
        offset += errorMessageLength;
      }

      datanodeContainerInfoList.add(
          new DatanodeContainerInfo(containerStateTimestamp, state, containerBCSID, errorMessage));
    }

    return datanodeContainerInfoList;
  }

  private byte[] concatenateByteArrays(byte[]... arrays) {
    int totalLength = 0;
    for (byte[] array : arrays) {
      totalLength += array.length;
    }

    byte[] result = new byte[totalLength];
    int offset = 0;
    for (byte[] array : arrays) {
      System.arraycopy(array, 0, result, offset, array.length);
      offset += array.length;
    }

    return result;
  }

  private byte[] combineByteArrays(List<byte[]> byteArrays) {
    int totalLength = 0;
    for (byte[] byteArray : byteArrays) {
      totalLength += byteArray.length;
    }

    byte[] result = new byte[totalLength];
    int offset = 0;
    for (byte[] byteArray : byteArrays) {
      System.arraycopy(byteArray, 0, result, offset, byteArray.length);
      offset += byteArray.length;
    }

    return result;
  }

  private byte[] sliceArray(byte[] array, int offset, int length) {
    // Check that the offset and length are within valid bounds
    if (offset < 0 || offset + length > array.length) {
      throw new ArrayIndexOutOfBoundsException("Invalid offset or length: offset=" +
          offset + ", length=" + length + ", array.length=" + array.length);
    }

    byte[] slicedArray = new byte[length];
    System.arraycopy(array, offset, slicedArray, 0, length);
    return slicedArray;
  }

}

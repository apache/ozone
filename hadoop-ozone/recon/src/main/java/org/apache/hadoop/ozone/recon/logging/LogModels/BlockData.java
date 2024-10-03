package org.apache.hadoop.ozone.recon.logging.LogModels;


import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * This class encapsulates a block data
 * It will contain a byte array <code>data</code> to store the string characters in a block
 * It also contains a long <code>offset</code> which stores the offset after the block
 */
public class BlockData {
  private byte[] data;
  private long offset;

  public BlockData(byte[] data, long offset) {
    this.data = data;
    this.offset = offset;
  }

  public byte[] getData() {
    return this.data;
  }

  public void setData(byte[] data) {
    this.data = data;
  }

  public long getOffset() {
    return this.offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  /**
   * Return the string representation of byte array in UTF_8
   */
  public String getDataAsString() {
    return new String(data, StandardCharsets.UTF_8);
  }

  public List<String> getLinesFromBlock() throws IOException {
    List<String> lines = new ArrayList<>();
    ByteArrayInputStream stream = new ByteArrayInputStream(this.data);
    InputStreamReader streamReader = new InputStreamReader(stream, StandardCharsets.UTF_8);
    try (BufferedReader bufferedReader = new BufferedReader(streamReader)) {
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        lines.add(line);
      }
    }
    return  lines;
  }

}

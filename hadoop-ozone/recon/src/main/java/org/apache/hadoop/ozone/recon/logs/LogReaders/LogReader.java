package org.apache.hadoop.ozone.recon.logs.LogReaders;


import com.google.common.primitives.Bytes;
import org.apache.hadoop.ozone.recon.logs.LogModels.BlockData;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * This class provides us functions to read log files by implementing
 * a ArrayList acting as a buffer and reading the log file in blocks
 * along with utilities to shift the file pointer for efficient parsing
 * of large log files
 */
public class LogReader {
  // Size of the character buffer is 4096 bytes or 4 Kbytes
  // This is a standard block size of UFS file system
  private static final int BLOCK_SIZE = 4096;
  // The number of bytes to read at a time when searching for a newline
  private static final int NEWLINE_BUFFER_SIZE = 256;
  // Store the position in the file
  private long currPos = 0L;
  // Store the position of the line
  private int currLinePos = 0;

  private int lastBlockSize = 0;
  //This will be an ArrayList to act as the buffer for our strings
  private List<String> lines;
  private final long fileSize;

  private enum Direction {
    FORWARD,
    REVERSE,
    NEUTRAL
  }

  // The direction in which the line and blocks were last read
  private Direction lineDirection;
  private Direction blockDirection;
  private final RandomAccessFile raf;

  /**
   * see {@link RandomAccessFile#RandomAccessFile(File,String)}
   * @param file  Stores the File to read
   * @param mode  Stores the mode in which we open the file
   * @throws IOException if the file is not present or error occurs during I/O
   * @throws SecurityException if the file is not having proper permissions
   */
  public LogReader(File file, String mode)
    throws IOException, SecurityException {
    raf = new RandomAccessFile(file, mode);
    lineDirection = Direction.NEUTRAL;
    blockDirection = Direction.NEUTRAL;
    lines = new ArrayList<String>();
    fileSize = raf.length();

  }

  /**
   * Method to reset all buffer related data
   */
  public void resetBuffers() {
    lines.clear();
    currPos = 0;
    lastBlockSize = 0;
    lineDirection = Direction.NEUTRAL;
    blockDirection = Direction.NEUTRAL;
  }

  /**
   * Read the file till the previous new line or line feed is encountered
   * @return An array of bytes storing the data till newline or null if there is no data
   * @throws IOException in case of I/O errors
   */
  public byte[] readTillPrevLF() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    // Adjusted buffer size, this will change based on if we reached file start
    long buffSize = NEWLINE_BUFFER_SIZE;

    // Empty byte array to store final data
    // We cannot stream the data normally since we are reading in reverse
    // Hence we need to either reverse the data order or add new data before the current array
    byte[] data = new byte[0];

    for(;;) {
      long filePos = raf.getFilePointer();
      // We are at the beginning
      if (0 == filePos) {
        break;
      }
      // If the current buffer size exceeds the pointer in file
      // Reset to the pointer position size
      buffSize = Math.min(buffSize, filePos);
      byte[] currData = new byte[(int) buffSize];

      // Go back by the buffSize length in the file
      raf.seek(raf.getFilePointer() - buffSize);
      int numOfBytesRead = raf.read(currData);
      // No data to read
      if (numOfBytesRead <= 0) {
        break;
      }
      int newLineIdx = Bytes.lastIndexOf(currData, (byte) '\n');
      if (-1 == newLineIdx) {
        // Didn't find newline, prepend current byte data to final data
        data = Bytes.concat(currData, data);
        // Go back more beyond the read data
        raf.seek(raf.getFilePointer() - numOfBytesRead - buffSize);
      } else {
        // Write the data till newline
        for (int i = 0; i < newLineIdx; i++) {
          baos.write(data[i]);
        }
        data = Bytes.concat(baos.toByteArray(), data);
        baos.reset();
        raf.seek(raf.getFilePointer() - (buffSize - newLineIdx) + 1);
        break;
      }
    }
    return (data.length > 0) ? data : null;
  }

  /**
   * Read the file till the next new line or line feed is encountered
   * @return An array of bytes storing the data till the newline or null if there is no data
   * @throws IOException in case of I/O errors
   */
  public byte[] readTillNextLF() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for(;;) {
      byte[] data = new byte[NEWLINE_BUFFER_SIZE];
      int numOfBytesRead = raf.read(data);
      // No data to read
      if (numOfBytesRead <= 0) {
        break;
      }
      int newLineIdx = Bytes.indexOf(data, (byte) '\n');
      // Newline was not found
      if (-1 == newLineIdx) {
        // Didn't find newline, flush data to stream
        baos.write(data);
      } else {
        // Write data till newline
        for (int i = 0; i < newLineIdx; i++){
          baos.write(data[i]);
        }
        // Change the file position to current cursor position to the byte after newline
        raf.seek((raf.getFilePointer() - (NEWLINE_BUFFER_SIZE - newLineIdx) + 1));
        break;
      }
    }
    return (baos.size() > 0) ? baos.toByteArray() : null;
  }

  /**
   * Read the previous block of data till the previous line feed / new line is encountered
   * @return {@link BlockData} with byte array of the data and offset position of the block
   * @throws IOException if I/O operation error occurs
   */
  public BlockData readPreviousBlock() throws IOException {
    // If the last block was read in the forwards direction we need
    // to adjust position to beginning of the block
    if (Direction.FORWARD == blockDirection) {
      raf.seek(raf.getFilePointer() - lastBlockSize);
    }

    // If we are at the beginning of the file, there is nothing more to go back
    if (0 == raf.getFilePointer()){
      resetBuffers();
      return null;
    }

    // Seek to the start of the block to read
    long blockSize = Math.min(raf.getFilePointer(), BLOCK_SIZE);
    raf.seek(raf.getFilePointer() - blockSize);
    long offset = raf.getFilePointer();
    byte[] blockData = new byte[(int) blockSize];
    int numOfBytesRead = raf.read(blockData);
    if (numOfBytesRead > 0) {
      // Go back to block start position
      raf.seek(raf.getFilePointer() - blockSize);
      // Fetch the previous new line before this block
      byte[] dataTillLF = readTillPrevLF();
      if (null == dataTillLF) {
        return null;
      }
      // Prepend the previous data till new line
      blockData = Bytes.concat(dataTillLF, blockData);
      // The offset should go back by the extra new line data size
      offset -= dataTillLF.length;
      lastBlockSize = blockData.length;
      blockDirection = Direction.REVERSE;
    }
    return new BlockData(blockData, offset);
  }

  /**
   * Read the next block of data
   * @return {@link BlockData} with the byte array of data and offset position for the block
   * @throws IOException if I/O operation error occurs
   */
  public BlockData readNextBlock() throws IOException {
    // If the last block was read in the reverse direction we need
    // to adjust the position to the end of the block
    if (Direction.REVERSE == blockDirection) {
      raf.seek((raf.getFilePointer() + lastBlockSize));
    }

    // If we are at the end of the file there is nothing more to be read
    if (raf.getFilePointer() == fileSize) {
      resetBuffers();
      return null;
    }

    long offset = raf.getFilePointer();
    byte[] blockData = new byte[BLOCK_SIZE];
    int numBytesRead = raf.read(blockData);
    //Check if the last byte is newline or not
    if (numBytesRead > 0 && ('\n' != (char) blockData[blockData.length - 1])) {
      byte[] dataTillLF = readTillNextLF();
      if (null == dataTillLF) {
        return null;
      }
      blockData = Bytes.concat(blockData, dataTillLF);
    }
    lastBlockSize = blockData.length;
    blockDirection = Direction.FORWARD;

    return new BlockData(blockData, offset);
  }

  public String getPrevLine() throws IOException {
    // If we are reading in the FORWARD direction, decrement position by 1
    // to avoid re-read of the next line from previous getNextLine call
    if(Direction.FORWARD == lineDirection) {
      currLinePos -= 1;
    }

    // If the position is at the beginning of the line buffer
    // Load in the previous block of lines
    if (currPos == 0) {
      BlockData prevBlockData = readPreviousBlock();
      if (null == prevBlockData) {
        resetBuffers();
        return null;
      }
      lines = prevBlockData.getLinesFromBlock();
      currLinePos = lines.size();
    }
    currLinePos -= 1;
    String line = lines.get(currLinePos);
    lineDirection = Direction.REVERSE;
    return line;
  }

  public String getNextLine() throws IOException {
    // If we were reading in the REVERSE direction, increase position by 1
    // to avoid re-read of the last line from previous getPrevLine call
    if (Direction.REVERSE == lineDirection) {
      currLinePos += 1;
    }

    // If the position is more than the current buffer size
    // Load in the next block of lines
    if (currLinePos >= lines.size()) {
      BlockData nextBlockData = readNextBlock();
      if (null == nextBlockData) {
        resetBuffers();
        return null;
      }
      lines = nextBlockData.getLinesFromBlock();
      currLinePos = 0;
    }
    String line = lines.get(currLinePos);
    currLinePos += 1;
    lineDirection = Direction.FORWARD;
    return line;
  }

  /**
   * Get the current pointer position in the file
   * @return The offset value representing the pointer
   * @throws IOException in case of any I/O related error
   */
  public long getCurrentOffset() throws IOException{
    return raf.getFilePointer();
  }

  /**
   * Close any resources related to RandomFileAccess
   * @throws IOException in case something goes wrong while closing
   */
  public void close() throws IOException {
    raf.close();
  }

}

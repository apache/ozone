/**
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

package org.apache.hadoop.ozone.recon;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.recon.ReconUtils.createTarFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.net.URL;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
/**
 * Test Recon Utility methods.
 */
public class TestReconUtils {

  @TempDir
  private Path temporaryFolder;

  @Test
  public void testGetReconDbDir() throws Exception {

    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set("TEST_DB_DIR", temporaryFolder.toString());

    File file = new ReconUtils().getReconDbDir(configuration,
        "TEST_DB_DIR");
    assertEquals(temporaryFolder.toString(), file.getAbsolutePath());
  }

  @Test
  public void testCreateTarFile(@TempDir File tempSnapshotDir)
      throws Exception {

    FileInputStream fis = null;
    FileOutputStream fos = null;
    File tarFile = null;

    try {
      String testDirName = tempSnapshotDir.getPath();

      File file = new File(testDirName + "/temp1.txt");
      OutputStreamWriter writer = new OutputStreamWriter(
          new FileOutputStream(file), UTF_8);
      writer.write("Test data 1");
      writer.close();

      file = new File(testDirName + "/temp2.txt");
      writer = new OutputStreamWriter(
          new FileOutputStream(file), UTF_8);
      writer.write("Test data 2");
      writer.close();

      tarFile = createTarFile(Paths.get(testDirName));
      assertNotNull(tarFile);

    } finally {
      org.apache.hadoop.io.IOUtils.closeStream(fis);
      org.apache.hadoop.io.IOUtils.closeStream(fos);
      FileUtils.deleteDirectory(tempSnapshotDir);
      FileUtils.deleteQuietly(tarFile);
    }
  }

  @Test
  public void testUntarCheckpointFile() throws Exception {

    File newDir = Files.createDirectory(
        temporaryFolder.resolve("NewDir")).toFile();
    File file1 = Paths.get(newDir.getAbsolutePath(), "file1")
        .toFile();
    String str = "File1 Contents";
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(file1.getAbsoluteFile()), UTF_8));
    writer.write(str);
    writer.close();

    File file2 = Paths.get(newDir.getAbsolutePath(), "file2")
        .toFile();
    str = "File2 Contents";
    writer = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(file2.getAbsoluteFile()), UTF_8));
    writer.write(str);
    writer.close();

    //Create test tar file.
    File tarFile = createTarFile(newDir.toPath());
    File outputDir = Files.createDirectory(
        temporaryFolder.resolve("OutputDir")).toFile();
    new ReconUtils().untarCheckpointFile(tarFile, outputDir.toPath());

    assertTrue(outputDir.isDirectory());
    assertEquals(2, outputDir.listFiles().length);
  }

  @Test
  public void testMakeHttpCall() throws Exception {
    String url = "http://localhost:9874/dbCheckpoint";
    File file1 = Paths.get(temporaryFolder.toString(), "file1")
        .toFile();
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(file1.getAbsoluteFile()), UTF_8));
    writer.write("File 1 Contents");
    writer.close();
    InputStream fileInputStream = new FileInputStream(file1);

    String contents;
    URLConnectionFactory connectionFactoryMock =
        mock(URLConnectionFactory.class);
    HttpURLConnection urlConnectionMock = mock(HttpURLConnection.class);
    when(urlConnectionMock.getInputStream()).thenReturn(fileInputStream);
    when(connectionFactoryMock.openConnection(any(URL.class), anyBoolean()))
        .thenReturn(urlConnectionMock);
    try (InputStream inputStream = new ReconUtils()
        .makeHttpCall(connectionFactoryMock, url, false).getInputStream()) {
      contents = IOUtils.toString(inputStream, Charset.defaultCharset());
    }

    assertEquals("File 1 Contents", contents);
  }

  @Test
  public void testGetLastKnownDB(@TempDir File newDir) throws IOException {

    File file1 = Paths.get(newDir.getAbsolutePath(), "valid_1")
        .toFile();
    String str = "File1 Contents";
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(file1.getAbsoluteFile()), UTF_8));
    writer.write(str);
    writer.close();

    File file2 = Paths.get(newDir.getAbsolutePath(), "valid_2")
        .toFile();
    str = "File2 Contents";
    writer = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(file2.getAbsoluteFile()), UTF_8));
    writer.write(str);
    writer.close();


    File file3 = Paths.get(newDir.getAbsolutePath(), "invalid_3")
        .toFile();
    str = "File3 Contents";
    writer = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(file3.getAbsoluteFile()), UTF_8));
    writer.write(str);
    writer.close();

    ReconUtils reconUtils = new ReconUtils();
    File latestValidFile = reconUtils.getLastKnownDB(newDir, "valid");
    assertEquals("valid_2", latestValidFile.getName());
  }

  @Test
  public void testNextClosestPowerIndexOfTwo() {
    assertNextClosestPowerIndexOfTwo(0);
    assertNextClosestPowerIndexOfTwo(1);
    assertNextClosestPowerIndexOfTwo(-1);
    assertNextClosestPowerIndexOfTwo(Long.MAX_VALUE);
    assertNextClosestPowerIndexOfTwo(Long.MIN_VALUE);

    for (long n = 1; n != 0; n <<= 1) {
      assertNextClosestPowerIndexOfTwo(n);
      assertNextClosestPowerIndexOfTwo(n + 1);
      assertNextClosestPowerIndexOfTwo(n - 1);
    }

    final Random random = new Random();
    for (int i = 0; i < 10; i++) {
      assertNextClosestPowerIndexOfTwo(random.nextLong());
    }
  }

  static void assertNextClosestPowerIndexOfTwo(long n) {
    final int expected = oldNextClosestPowerIndexOfTwoFixed(n);
    final int computed = ReconUtils.nextClosestPowerIndexOfTwo(n);
    assertEquals(expected, computed, "n=" + n);
  }

  private static int oldNextClosestPowerIndexOfTwoFixed(long n) {
    return n == 0 ? 0
        : n == Long.MIN_VALUE ? -63
        : n == Long.highestOneBit(n) ? 63 - Long.numberOfLeadingZeros(n)
        : n > 0 ? oldNextClosestPowerIndexOfTwo(n)
        : -oldNextClosestPowerIndexOfTwoFixed(-n);
  }

  /** The old buggy method works only for n >= 0 with n not a power of 2. */
  private static int oldNextClosestPowerIndexOfTwo(long dataSize) {
    int index = 0;
    while (dataSize != 0) {
      dataSize >>= 1;
      index += 1;
    }
    return index;
  }
}

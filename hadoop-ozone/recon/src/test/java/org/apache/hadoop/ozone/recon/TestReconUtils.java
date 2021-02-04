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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
import java.nio.file.Paths;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
/**
 * Test Recon Utility methods.
 */
public class TestReconUtils {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testGetReconDbDir() throws Exception {

    String filePath = folder.getRoot().getAbsolutePath();
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set("TEST_DB_DIR", filePath);

    File file = new ReconUtils().getReconDbDir(configuration,
        "TEST_DB_DIR");
    Assert.assertEquals(filePath, file.getAbsolutePath());
  }

  @Test
  public void testCreateTarFile() throws Exception {

    File tempSnapshotDir = null;
    FileInputStream fis = null;
    FileOutputStream fos = null;
    File tarFile = null;

    try {
      String testDirName = System.getProperty("java.io.tmpdir");
      if (!testDirName.endsWith("/")) {
        testDirName += "/";
      }
      testDirName += "TestCreateTarFile_Dir" + System.currentTimeMillis();
      tempSnapshotDir = new File(testDirName);
      tempSnapshotDir.mkdirs();

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
      Assert.assertNotNull(tarFile);

    } finally {
      org.apache.hadoop.io.IOUtils.closeStream(fis);
      org.apache.hadoop.io.IOUtils.closeStream(fos);
      FileUtils.deleteDirectory(tempSnapshotDir);
      FileUtils.deleteQuietly(tarFile);
    }
  }

  @Test
  public void testUntarCheckpointFile() throws Exception {

    File newDir = folder.newFolder();

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
    File outputDir = folder.newFolder();
    new ReconUtils().untarCheckpointFile(tarFile, outputDir.toPath());

    assertTrue(outputDir.isDirectory());
    assertTrue(outputDir.listFiles().length == 2);
  }

  @Test
  public void testMakeHttpCall() throws Exception {
    String url = "http://localhost:9874/dbCheckpoint";
    File file1 = Paths.get(folder.getRoot().getPath(), "file1")
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
  public void testGetLastKnownDB() throws IOException {
    File newDir = folder.newFolder();

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
    assertTrue(latestValidFile.getName().equals("valid_2"));
  }
}

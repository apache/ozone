/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerPacker;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.compress.compressors.CompressorStreamFactory.GZIP;

/**
 * Test the tar/untar for a given container.
 */
@RunWith(Parameterized.class)
public class TestTarContainerPacker {

  private static final String TEST_DB_FILE_NAME = "test1";

  private static final String TEST_DB_FILE_CONTENT = "test1";

  private static final String TEST_CHUNK_FILE_NAME = "chunk1";

  private static final String TEST_CHUNK_FILE_CONTENT = "This is a chunk";

  private static final String TEST_DESCRIPTOR_FILE_CONTENT = "descriptor";

  private final ContainerPacker<KeyValueContainerData> packer
      = new TarContainerPacker();

  private static final Path SOURCE_CONTAINER_ROOT =
      Paths.get("target/test/data/packer-source-dir");

  private static final Path DEST_CONTAINER_ROOT =
      Paths.get("target/test/data/packer-dest-dir");

  private static final Path TEMP_DIR =
      Paths.get("target/test/data/packer-tmp-dir");

  private static final AtomicInteger CONTAINER_ID = new AtomicInteger(1);

  private final ChunkLayOutVersion layout;

  public TestTarContainerPacker(ChunkLayOutVersion layout) {
    this.layout = layout;
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> parameters() {
    return ChunkLayoutTestInfo.chunkLayoutParameters();
  }

  @BeforeClass
  public static void init() throws IOException {
    initDir(SOURCE_CONTAINER_ROOT);
    initDir(DEST_CONTAINER_ROOT);
    initDir(TEMP_DIR);
  }

  @AfterClass
  public static void cleanup() throws IOException {
    FileUtils.deleteDirectory(SOURCE_CONTAINER_ROOT.toFile());
    FileUtils.deleteDirectory(DEST_CONTAINER_ROOT.toFile());
    FileUtils.deleteDirectory(TEMP_DIR.toFile());
  }

  private static void initDir(Path path) throws IOException {
    if (path.toFile().exists()) {
      FileUtils.deleteDirectory(path.toFile());
    }
    Files.createDirectories(path);
  }

  private KeyValueContainerData createContainer(Path dir) throws IOException {
    long id = CONTAINER_ID.getAndIncrement();

    Path containerDir = dir.resolve("container" + id);
    Path dbDir = containerDir.resolve("db");
    Path dataDir = containerDir.resolve("data");
    Files.createDirectories(dbDir);
    Files.createDirectories(dataDir);

    KeyValueContainerData containerData = new KeyValueContainerData(
        id, layout,
        -1, UUID.randomUUID().toString(), UUID.randomUUID().toString());
    containerData.setChunksPath(dataDir.toString());
    containerData.setMetadataPath(dbDir.getParent().toString());
    containerData.setDbFile(dbDir.toFile());

    return containerData;
  }

  @Test
  public void pack() throws IOException, CompressorException {

    //GIVEN
    OzoneConfiguration conf = new OzoneConfiguration();

    KeyValueContainerData sourceContainerData =
        createContainer(SOURCE_CONTAINER_ROOT);

    KeyValueContainer sourceContainer =
        new KeyValueContainer(sourceContainerData, conf);

    //sample db file in the metadata directory
    writeDbFile(sourceContainerData, TEST_DB_FILE_NAME);

    //sample chunk file in the chunk directory
    writeChunkFile(sourceContainerData, TEST_CHUNK_FILE_NAME);

    //sample container descriptor file
    writeDescriptor(sourceContainer);

    Path targetFile = TEMP_DIR.resolve("container.tar.gz");

    //WHEN: pack it
    try (FileOutputStream output = new FileOutputStream(targetFile.toFile())) {
      packer.pack(sourceContainer, output);
    }

    //THEN: check the result
    TarArchiveInputStream tarStream = null;
    try (FileInputStream input = new FileInputStream(targetFile.toFile())) {
      CompressorInputStream uncompressed = new CompressorStreamFactory()
          .createCompressorInputStream(GZIP, input);
      tarStream = new TarArchiveInputStream(uncompressed);

      TarArchiveEntry entry;
      Map<String, TarArchiveEntry> entries = new HashMap<>();
      while ((entry = tarStream.getNextTarEntry()) != null) {
        entries.put(entry.getName(), entry);
      }

      Assert.assertTrue(
          entries.containsKey("container.yaml"));

    } finally {
      if (tarStream != null) {
        tarStream.close();
      }
    }

    //read the container descriptor only
    try (FileInputStream input = new FileInputStream(targetFile.toFile())) {
      String containerYaml = new String(packer.unpackContainerDescriptor(input),
          UTF_8);
      Assert.assertEquals(TEST_DESCRIPTOR_FILE_CONTENT, containerYaml);
    }

    KeyValueContainerData destinationContainerData =
        createContainer(DEST_CONTAINER_ROOT);

    KeyValueContainer destinationContainer =
        new KeyValueContainer(destinationContainerData, conf);

    String descriptor;

    //unpackContainerData
    try (FileInputStream input = new FileInputStream(targetFile.toFile())) {
      descriptor =
          new String(packer.unpackContainerData(destinationContainer, input),
              UTF_8);
    }

    assertExampleMetadataDbIsGood(
        destinationContainerData.getDbFile().toPath(),
        TEST_DB_FILE_NAME);
    assertExampleChunkFileIsGood(
        Paths.get(destinationContainerData.getChunksPath()),
        TEST_CHUNK_FILE_NAME);
    Assert.assertFalse(
        "Descriptor file should not have been extracted by the "
            + "unpackContainerData Call",
        destinationContainer.getContainerFile().exists());
    Assert.assertEquals(TEST_DESCRIPTOR_FILE_CONTENT, descriptor);
  }

  @Test
  public void unpackContainerDataWithValidRelativeDbFilePath()
      throws Exception {
    //GIVEN
    KeyValueContainerData sourceContainerData =
        createContainer(SOURCE_CONTAINER_ROOT);

    String fileName = "sub/dir/" + TEST_DB_FILE_NAME;
    File file = writeDbFile(sourceContainerData, fileName);
    String entryName = TarContainerPacker.DB_DIR_NAME + "/" + fileName;

    File containerFile = packContainerWithSingleFile(file, entryName);

    // WHEN
    KeyValueContainerData dest = unpackContainerData(containerFile);

    // THEN
    assertExampleMetadataDbIsGood(dest.getDbFile().toPath(), fileName);
  }

  @Test
  public void unpackContainerDataWithValidRelativeChunkFilePath()
      throws Exception {
    //GIVEN
    KeyValueContainerData sourceContainerData =
        createContainer(SOURCE_CONTAINER_ROOT);

    String fileName = "sub/dir/" + TEST_CHUNK_FILE_NAME;
    File file = writeChunkFile(sourceContainerData, fileName);
    String entryName = TarContainerPacker.CHUNKS_DIR_NAME + "/" + fileName;

    File containerFile = packContainerWithSingleFile(file, entryName);

    // WHEN
    KeyValueContainerData dest = unpackContainerData(containerFile);

    // THEN
    assertExampleChunkFileIsGood(Paths.get(dest.getChunksPath()), fileName);
  }

  @Test
  public void unpackContainerDataWithInvalidRelativeDbFilePath()
      throws Exception {
    //GIVEN
    KeyValueContainerData sourceContainerData =
        createContainer(SOURCE_CONTAINER_ROOT);

    String fileName = "../db_file";
    File file = writeDbFile(sourceContainerData, fileName);
    String entryName = TarContainerPacker.DB_DIR_NAME + "/" + fileName;

    File containerFile = packContainerWithSingleFile(file, entryName);

    LambdaTestUtils.intercept(IllegalArgumentException.class,
        () -> unpackContainerData(containerFile));
  }

  @Test
  public void unpackContainerDataWithInvalidRelativeChunkFilePath()
      throws Exception {
    //GIVEN
    KeyValueContainerData sourceContainerData =
        createContainer(SOURCE_CONTAINER_ROOT);

    String fileName = "../chunk_file";
    File file = writeChunkFile(sourceContainerData, fileName);
    String entryName = TarContainerPacker.CHUNKS_DIR_NAME + "/" + fileName;

    File containerFile = packContainerWithSingleFile(file, entryName);

    LambdaTestUtils.intercept(IllegalArgumentException.class,
        () -> unpackContainerData(containerFile));
  }

  private KeyValueContainerData unpackContainerData(File containerFile)
      throws IOException {
    try (FileInputStream input = new FileInputStream(containerFile)) {
      OzoneConfiguration conf = new OzoneConfiguration();
      KeyValueContainerData data = createContainer(DEST_CONTAINER_ROOT);
      KeyValueContainer container = new KeyValueContainer(data, conf);
      packer.unpackContainerData(container, input);
      return data;
    }
  }

  private void writeDescriptor(KeyValueContainer container) throws IOException {
    FileOutputStream fileStream = new FileOutputStream(
        container.getContainerFile());
    try (OutputStreamWriter writer = new OutputStreamWriter(fileStream,
        UTF_8)) {
      IOUtils.write(TEST_DESCRIPTOR_FILE_CONTENT, writer);
    }
  }

  private File writeChunkFile(
      KeyValueContainerData containerData, String chunkFileName)
      throws IOException {
    Path path = Paths.get(containerData.getChunksPath())
        .resolve(chunkFileName);
    Files.createDirectories(path.getParent());
    File file = path.toFile();
    FileOutputStream fileStream = new FileOutputStream(file);
    try (OutputStreamWriter writer = new OutputStreamWriter(fileStream,
        UTF_8)) {
      IOUtils.write(TEST_CHUNK_FILE_CONTENT, writer);
    }
    return file;
  }

  private File writeDbFile(
      KeyValueContainerData containerData, String dbFileName)
      throws IOException {
    Path path = containerData.getDbFile().toPath()
        .resolve(dbFileName);
    Files.createDirectories(path.getParent());
    File file = path.toFile();
    FileOutputStream fileStream = new FileOutputStream(file);
    try (OutputStreamWriter writer = new OutputStreamWriter(fileStream,
        UTF_8)) {
      IOUtils.write(TEST_DB_FILE_CONTENT, writer);
    }
    return file;
  }

  private File packContainerWithSingleFile(File file, String entryName)
      throws Exception {
    File targetFile = TEMP_DIR.resolve("container.tar.gz").toFile();
    try (FileOutputStream output = new FileOutputStream(targetFile);
         CompressorOutputStream gzipped = new CompressorStreamFactory()
             .createCompressorOutputStream(GZIP, output);
         ArchiveOutputStream archive = new TarArchiveOutputStream(gzipped)) {
      TarContainerPacker.includeFile(file, entryName, archive);
    }
    return targetFile;
  }

  private void assertExampleMetadataDbIsGood(Path dbPath, String filename)
      throws IOException {

    Path dbFile = dbPath.resolve(filename);

    Assert.assertTrue(
        "example DB file is missing after pack/unpackContainerData: " + dbFile,
        Files.exists(dbFile));

    try (FileInputStream testFile = new FileInputStream(dbFile.toFile())) {
      List<String> strings = IOUtils.readLines(testFile, UTF_8);
      Assert.assertEquals(1, strings.size());
      Assert.assertEquals(TEST_DB_FILE_CONTENT, strings.get(0));
    }
  }

  private void assertExampleChunkFileIsGood(Path chunkPath, String filename)
      throws IOException {

    Path chunkFile = chunkPath.resolve(filename);

    Assert.assertTrue(
        "example chunk file is missing after pack/unpackContainerData: "
            + chunkFile,
        Files.exists(chunkFile));

    try (FileInputStream testFile = new FileInputStream(chunkFile.toFile())) {
      List<String> strings = IOUtils.readLines(testFile, UTF_8);
      Assert.assertEquals(1, strings.size());
      Assert.assertEquals(TEST_CHUNK_FILE_CONTENT, strings.get(0));
    }
  }

}

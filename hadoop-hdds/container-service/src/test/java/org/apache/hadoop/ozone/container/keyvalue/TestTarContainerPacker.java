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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.ozone.container.replication.CopyContainerCompression;
import org.apache.ozone.test.SpyInputStream;
import org.apache.ozone.test.SpyOutputStream;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.newInputStream;
import static java.nio.file.Files.newOutputStream;
import static org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker.CONTAINER_FILE_NAME;
import static org.junit.Assert.assertThrows;

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

  private TarContainerPacker packer;

  private static final Path SOURCE_CONTAINER_ROOT =
      Paths.get("target/test/data/packer-source-dir");

  private static final Path DEST_CONTAINER_ROOT =
      Paths.get("target/test/data/packer-dest-dir");

  private static final Path TEMP_DIR =
      Paths.get("target/test/data/packer-tmp-dir");

  private static final AtomicInteger CONTAINER_ID = new AtomicInteger(1);

  private final ContainerLayoutVersion layout;
  private final String schemaVersion;
  private OzoneConfiguration conf;

  public TestTarContainerPacker(ContainerTestVersionInfo versionInfo,
      CopyContainerCompression compression) {
    this.layout = versionInfo.getLayout();
    this.schemaVersion = versionInfo.getSchemaVersion();
    this.conf = new OzoneConfiguration();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);
    packer = new TarContainerPacker(compression);

  }

  @Parameterized.Parameters
  public static Iterable<Object[]> parameters() {
    List<ContainerTestVersionInfo> layoutList =
        ContainerTestVersionInfo.getLayoutList();
    List<Object[]> parameterList = new ArrayList<>();
    for (ContainerTestVersionInfo containerTestVersionInfo : layoutList) {
      for (CopyContainerCompression compr : CopyContainerCompression.values()) {
        parameterList.add(new Object[]{containerTestVersionInfo, compr});
      }
    }
    return parameterList;
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
    return createContainer(dir, true);
  }

  private KeyValueContainerData createContainer(Path dir, boolean createDir)
      throws IOException {
    long id = CONTAINER_ID.getAndIncrement();

    Path containerDir = dir.resolve(String.valueOf(id));
    Path dbDir = containerDir.resolve("db");
    Path dataDir = containerDir.resolve("chunks");
    Path metaDir = containerDir.resolve("metadata");
    if (createDir) {
      Files.createDirectories(metaDir);
      Files.createDirectories(dbDir);
      Files.createDirectories(dataDir);
    }

    KeyValueContainerData containerData = new KeyValueContainerData(
        id, layout,
        -1, UUID.randomUUID().toString(), UUID.randomUUID().toString());
    containerData.setSchemaVersion(schemaVersion);
    containerData.setChunksPath(dataDir.toString());
    containerData.setMetadataPath(metaDir.toString());
    containerData.setDbFile(dbDir.toFile());

    return containerData;
  }

  @Test
  public void pack() throws IOException {
    //GIVEN
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

    Path targetFile = TEMP_DIR.resolve("container.tar");

    //WHEN: pack it
    SpyOutputStream outputForPack =
        new SpyOutputStream(newOutputStream(targetFile));
    packer.pack(sourceContainer, outputForPack);

    //THEN: check the result
    TarArchiveInputStream tarStream = null;
    try (FileInputStream input = new FileInputStream(targetFile.toFile())) {
      InputStream uncompressed = packer.decompress(input);
      tarStream = new TarArchiveInputStream(uncompressed);

      boolean first = true;
      TarArchiveEntry entry;
      Map<String, TarArchiveEntry> entries = new HashMap<>();
      while ((entry = tarStream.getNextTarEntry()) != null) {
        if (first) {
          Assert.assertEquals(CONTAINER_FILE_NAME, entry.getName());
          first = false;
        }
        entries.put(entry.getName(), entry);
      }

      Assert.assertTrue(entries.containsKey(CONTAINER_FILE_NAME));
    } finally {
      if (tarStream != null) {
        tarStream.close();
      }
    }
    outputForPack.assertClosedExactlyOnce();

    //read the container descriptor only
    SpyInputStream inputForUnpackDescriptor =
        new SpyInputStream(newInputStream(targetFile));
    String containerYaml = new String(
        packer.unpackContainerDescriptor(inputForUnpackDescriptor),
        UTF_8);
    Assert.assertEquals(TEST_DESCRIPTOR_FILE_CONTENT, containerYaml);
    inputForUnpackDescriptor.assertClosedExactlyOnce();

    KeyValueContainerData destinationContainerData =
        createContainer(DEST_CONTAINER_ROOT, false);

    KeyValueContainer destinationContainer =
        new KeyValueContainer(destinationContainerData, conf);

    //unpackContainerData
    SpyInputStream inputForUnpackData =
        new SpyInputStream(newInputStream(targetFile));
    String descriptor = new String(
        packer.unpackContainerData(destinationContainer, inputForUnpackData,
            TEMP_DIR, DEST_CONTAINER_ROOT.resolve(String.valueOf(
                destinationContainer.getContainerData().getContainerID()))),
        UTF_8);

    assertExampleMetadataDbIsGood(
        TarContainerPacker.getDbPath(destinationContainerData),
        TEST_DB_FILE_NAME);
    assertExampleChunkFileIsGood(
        Paths.get(destinationContainerData.getChunksPath()),
        TEST_CHUNK_FILE_NAME);
    Assert.assertFalse(
        "Descriptor file should not have been extracted by the "
            + "unpackContainerData Call",
        destinationContainer.getContainerFile().exists());
    Assert.assertEquals(TEST_DESCRIPTOR_FILE_CONTENT, descriptor);
    inputForUnpackData.assertClosedExactlyOnce();
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
    assertExampleMetadataDbIsGood(
        TarContainerPacker.getDbPath(dest), fileName);
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

    assertThrows(IllegalArgumentException.class,
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

    assertThrows(IllegalArgumentException.class,
        () -> unpackContainerData(containerFile));
  }

  private KeyValueContainerData unpackContainerData(File containerFile)
      throws IOException {
    try (FileInputStream input = new FileInputStream(containerFile)) {
      KeyValueContainerData data = createContainer(DEST_CONTAINER_ROOT, false);
      KeyValueContainer container = new KeyValueContainer(data, conf);
      packer.unpackContainerData(container, input, TEMP_DIR,
          DEST_CONTAINER_ROOT.resolve(String.valueOf(data.getContainerID())));
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
    return writeSingleFile(Paths.get(containerData.getChunksPath()),
        chunkFileName, TEST_CHUNK_FILE_CONTENT);
  }

  private File writeDbFile(
      KeyValueContainerData containerData, String dbFileName)
      throws IOException {
    return writeSingleFile(TarContainerPacker.getDbPath(containerData),
        dbFileName, TEST_DB_FILE_CONTENT);
  }

  private File writeSingleFile(Path parentPath, String fileName,
      String content) throws IOException {
    Path path = parentPath.resolve(fileName);
    Files.createDirectories(path.getParent());
    File file = path.toFile();
    FileOutputStream fileStream = new FileOutputStream(file);
    try (OutputStreamWriter writer = new OutputStreamWriter(fileStream,
        UTF_8)) {
      IOUtils.write(content, writer);
    }
    return file;
  }

  private File packContainerWithSingleFile(File file, String entryName)
      throws Exception {
    File targetFile = TEMP_DIR.resolve("container.tar").toFile();
    try (FileOutputStream output = new FileOutputStream(targetFile);
         OutputStream compressed = packer.compress(output);
         TarArchiveOutputStream archive =
             new TarArchiveOutputStream(compressed)) {
      archive.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
      TarContainerPacker.includeFile(file, entryName, archive);
    }
    return targetFile;
  }

  private void assertExampleMetadataDbIsGood(Path dbPath, String filename)
      throws IOException {
    assertExampleFileIsGood(dbPath, filename, TEST_DB_FILE_CONTENT);
  }

  private void assertExampleChunkFileIsGood(Path chunkPath, String filename)
      throws IOException {
    assertExampleFileIsGood(chunkPath, filename, TEST_CHUNK_FILE_CONTENT);
  }

  private void assertExampleFileIsGood(Path parentPath, String filename,
      String content) throws IOException {

    Path exampleFile = parentPath.resolve(filename);

    Assert.assertTrue(
        "example file is missing after pack/unpackContainerData: "
            + exampleFile,
        Files.exists(exampleFile));

    try (FileInputStream testFile =
             new FileInputStream(exampleFile.toFile())) {
      List<String> strings = IOUtils.readLines(testFile, UTF_8);
      Assert.assertEquals(1, strings.size());
      Assert.assertEquals(content, strings.get(0));
    }
  }

}

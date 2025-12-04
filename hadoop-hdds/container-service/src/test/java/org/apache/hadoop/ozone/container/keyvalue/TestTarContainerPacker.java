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

package org.apache.hadoop.ozone.container.keyvalue;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.newInputStream;
import static java.nio.file.Files.newOutputStream;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.assertTreesSortedAndMatch;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.buildTestTree;
import static org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker.CONTAINER_FILE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
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
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.Archiver;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeWriter;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.replication.CopyContainerCompression;
import org.apache.ozone.test.SpyInputStream;
import org.apache.ozone.test.SpyOutputStream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test the tar/untar for a given container.
 */
public class TestTarContainerPacker {

  private static final String TEST_DB_FILE_NAME = "test1";

  private static final String TEST_DB_FILE_CONTENT = "test1";

  private static final String TEST_CHUNK_FILE_NAME = "chunk1";

  private static final String TEST_CHUNK_FILE_CONTENT = "This is a chunk";

  private static final String TEST_DESCRIPTOR_FILE_CONTENT = "!<KeyValueContainerData>\n" +
      "checksum: 5e4bea7286f96d88a5b3a745011ff9e4281a5221bfe564413215cd85871dcfd8\n" +
      "chunksPath: target/test-dir/MiniOzoneClusterImpl-23c1bb30-d86a-4f79-88dc-574d8259a5b3/ozone-meta/datanode-4" +
        "/data-0/hdds/23c1bb30-d86a-4f79-88dc-574d8259a5b3/current/containerDir0/1/chunks\n" +
      "containerDBType: RocksDB\n" +
      "containerID: 1\n" +
      "containerType: KeyValueContainer\n" +
      "layOutVersion: 2\n" +
      "maxSize: 5368709120\n" +
      "metadata: {}\n" +
      "metadataPath: target/test-dir/MiniOzoneClusterImpl-23c1bb30-d86a-4f79-88dc-574d8259a5b3/ozone-meta/datanode-4" +
        "/data-0/hdds/23c1bb30-d86a-4f79-88dc-574d8259a5b3/current/containerDir0/1/metadata\n" +
      "originNodeId: 25a48afa-f8d8-44ff-b268-642167e5354b\n" +
      "originPipelineId: d7faca81-407f-4a50-a399-bd478c9795e5\n" +
      "schemaVersion: '3'\n" +
      "state: CLOSED";

  private TarContainerPacker packer;

  @TempDir
  private Path sourceContainerRoot;

  @TempDir
  private Path destContainerRoot;

  @TempDir
  private Path tempDir;

  private static final AtomicInteger CONTAINER_ID = new AtomicInteger(1);

  private ContainerLayoutVersion layout;
  private String schemaVersion;
  private OzoneConfiguration conf;
  private ContainerChecksumTreeManager checksumTreeManager;

  private void initTests(ContainerTestVersionInfo versionInfo,
      CopyContainerCompression compression) {
    this.layout = versionInfo.getLayout();
    this.schemaVersion = versionInfo.getSchemaVersion();
    this.conf = new OzoneConfiguration();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);
    packer = new TarContainerPacker(compression);
    checksumTreeManager = new ContainerChecksumTreeManager(conf);
  }

  public static List<Arguments> getLayoutAndCompression() {
    List<ContainerTestVersionInfo> layoutList =
        ContainerTestVersionInfo.getLayoutList();
    List<Arguments> parameterList = new ArrayList<>();
    for (ContainerTestVersionInfo containerTestVersionInfo : layoutList) {
      for (CopyContainerCompression compr : CopyContainerCompression.values()) {
        parameterList.add(Arguments.of(containerTestVersionInfo, compr));
      }
    }
    return parameterList;
  }

  private KeyValueContainerData createContainer(Path dir) throws IOException {
    return createContainer(dir, true, true);
  }

  private KeyValueContainerData createContainer(Path dir, boolean createDir, boolean incrementId)
      throws IOException {
    long id = CONTAINER_ID.get();
    if (incrementId) {
      id = CONTAINER_ID.getAndIncrement();
    }

    Path containerDir = dir.resolve(String.valueOf(id));
    Path dataDir = containerDir.resolve("chunks");
    Path metaDir = containerDir.resolve("metadata");
    Path dbDir = metaDir.resolve("db");
    if (createDir) {
      Files.createDirectories(metaDir);
      Files.createDirectories(dbDir);
      Files.createDirectories(dataDir);
    }

    KeyValueContainerData containerData = new KeyValueContainerData(
        id, layout,
        1, UUID.randomUUID().toString(), UUID.randomUUID().toString());
    containerData.setSchemaVersion(schemaVersion);
    containerData.setChunksPath(dataDir.toString());
    containerData.setMetadataPath(metaDir.toString());
    containerData.setDbFile(dbDir.toFile());

    return containerData;
  }

  @ParameterizedTest
  @MethodSource("getLayoutAndCompression")
  public void pack(ContainerTestVersionInfo versionInfo,
      CopyContainerCompression compression) throws IOException {
    initTests(versionInfo, compression);
    //GIVEN
    KeyValueContainerData sourceContainerData =
        createContainer(sourceContainerRoot, true, false);

    KeyValueContainer sourceContainer =
        new KeyValueContainer(sourceContainerData, conf);

    //sample db file in the metadata directory
    writeDbFile(sourceContainerData, TEST_DB_FILE_NAME);

    //sample chunk file in the chunk directory
    writeChunkFile(sourceContainerData, TEST_CHUNK_FILE_NAME);

    //write container checksum file in the metadata directory
    ContainerMerkleTreeWriter treeWriter = buildTestTree(conf);
    checksumTreeManager.updateTree(sourceContainerData, treeWriter);
    assertTrue(ContainerChecksumTreeManager.getContainerChecksumFile(sourceContainerData).exists());

    //sample container descriptor file
    writeDescriptor(sourceContainer);

    Path targetFile = tempDir.resolve("container.tar");

    //WHEN: pack it
    SpyOutputStream outputForPack =
        new SpyOutputStream(newOutputStream(targetFile));
    packer.pack(sourceContainer, outputForPack);

    //THEN: check the result
    TarArchiveInputStream tarStream = null;
    try (InputStream input = newInputStream(targetFile)) {
      InputStream uncompressed = packer.decompress(input);
      tarStream = new TarArchiveInputStream(uncompressed);

      boolean first = true;
      TarArchiveEntry entry;
      Map<String, TarArchiveEntry> entries = new HashMap<>();
      while ((entry = tarStream.getNextTarEntry()) != null) {
        if (first) {
          assertEquals(CONTAINER_FILE_NAME, entry.getName());
          first = false;
        }
        entries.put(entry.getName(), entry);
      }

      assertThat(entries).containsKey(CONTAINER_FILE_NAME);
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
    assertEquals(TEST_DESCRIPTOR_FILE_CONTENT, containerYaml);
    inputForUnpackDescriptor.assertClosedExactlyOnce();

    KeyValueContainerData destinationContainerData =
        createContainer(destContainerRoot, false, false);

    KeyValueContainer destinationContainer =
        new KeyValueContainer(destinationContainerData, conf);

    //unpackContainerData
    SpyInputStream inputForUnpackData =
        new SpyInputStream(newInputStream(targetFile));
    String descriptor = new String(
        packer.unpackContainerData(destinationContainer, inputForUnpackData,
            tempDir, destContainerRoot.resolve(String.valueOf(
                destinationContainer.getContainerData().getContainerID()))),
        UTF_8);

    assertExampleMetadataDbIsGood(
        TarContainerPacker.getDbPath(destinationContainerData),
        TEST_DB_FILE_NAME);
    assertExampleChunkFileIsGood(
        Paths.get(destinationContainerData.getChunksPath()),
        TEST_CHUNK_FILE_NAME);

    assertEquals(sourceContainerData.getContainerID(), destinationContainerData.getContainerID());
    assertTrue(ContainerChecksumTreeManager.getContainerChecksumFile(destinationContainerData).exists());
    assertTreesSortedAndMatch(checksumTreeManager.read(sourceContainerData).getContainerMerkleTree(),
        checksumTreeManager.read(destinationContainerData).getContainerMerkleTree());

    String containerFileData = new String(Files.readAllBytes(destinationContainer.getContainerFile().toPath()), UTF_8);
    assertTrue(containerFileData.contains("RECOVERING"),
        "The state of the container is not 'RECOVERING' in the container file");
    assertEquals(TEST_DESCRIPTOR_FILE_CONTENT, descriptor);
    inputForUnpackData.assertClosedExactlyOnce();
  }

  @ParameterizedTest
  @MethodSource("getLayoutAndCompression")
  public void unpackContainerDataWithValidRelativeDbFilePath(
      ContainerTestVersionInfo versionInfo,
      CopyContainerCompression compression)
      throws Exception {
    initTests(versionInfo, compression);
    //GIVEN
    KeyValueContainerData sourceContainerData =
        createContainer(sourceContainerRoot);

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

  @ParameterizedTest
  @MethodSource("getLayoutAndCompression")
  public void unpackContainerDataWithValidRelativeChunkFilePath(
      ContainerTestVersionInfo versionInfo,
      CopyContainerCompression compression)
      throws Exception {
    initTests(versionInfo, compression);
    //GIVEN
    KeyValueContainerData sourceContainerData =
        createContainer(sourceContainerRoot);

    String fileName = "sub/dir/" + TEST_CHUNK_FILE_NAME;
    File file = writeChunkFile(sourceContainerData, fileName);
    String entryName = TarContainerPacker.CHUNKS_DIR_NAME + "/" + fileName;

    File containerFile = packContainerWithSingleFile(file, entryName);

    // WHEN
    KeyValueContainerData dest = unpackContainerData(containerFile);

    // THEN
    assertExampleChunkFileIsGood(Paths.get(dest.getChunksPath()), fileName);
  }

  @ParameterizedTest
  @MethodSource("getLayoutAndCompression")
  public void unpackContainerDataWithInvalidRelativeDbFilePath(
      ContainerTestVersionInfo versionInfo,
      CopyContainerCompression compression)
      throws Exception {
    initTests(versionInfo, compression);
    //GIVEN
    KeyValueContainerData sourceContainerData =
        createContainer(sourceContainerRoot);

    String fileName = "../db_file";
    File file = writeDbFile(sourceContainerData, fileName);
    String entryName = TarContainerPacker.DB_DIR_NAME + "/" + fileName;

    File containerFile = packContainerWithSingleFile(file, entryName);

    assertThrows(IllegalArgumentException.class,
        () -> unpackContainerData(containerFile));
  }

  @ParameterizedTest
  @MethodSource("getLayoutAndCompression")
  public void unpackContainerDataWithInvalidRelativeChunkFilePath(
      ContainerTestVersionInfo versionInfo,
      CopyContainerCompression compression)
      throws Exception {
    initTests(versionInfo, compression);
    //GIVEN
    KeyValueContainerData sourceContainerData =
        createContainer(sourceContainerRoot);

    String fileName = "../chunk_file";
    File file = writeChunkFile(sourceContainerData, fileName);
    String entryName = TarContainerPacker.CHUNKS_DIR_NAME + "/" + fileName;

    File containerFile = packContainerWithSingleFile(file, entryName);

    assertThrows(IllegalArgumentException.class,
        () -> unpackContainerData(containerFile));
  }

  private KeyValueContainerData unpackContainerData(File containerFile)
      throws IOException {
    try (InputStream input = newInputStream(containerFile.toPath())) {
      KeyValueContainerData data = createContainer(destContainerRoot, false, true);
      KeyValueContainer container = new KeyValueContainer(data, conf);
      packer.unpackContainerData(container, input, tempDir,
          destContainerRoot.resolve(String.valueOf(data.getContainerID())));
      return data;
    }
  }

  private void writeDescriptor(KeyValueContainer container) throws IOException {
    try (OutputStream fileStream = newOutputStream(container.getContainerFile().toPath());
        OutputStreamWriter writer = new OutputStreamWriter(fileStream, UTF_8)) {
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
    Path path = parentPath.resolve(fileName).normalize();
    Path parent = path.getParent();
    assertNotNull(parent);
    Files.createDirectories(parent);
    File file = path.toFile();
    try (OutputStream fileStream = newOutputStream(file.toPath());
        OutputStreamWriter writer = new OutputStreamWriter(fileStream, UTF_8)) {
      IOUtils.write(content, writer);
    }
    return file;
  }

  private File packContainerWithSingleFile(File file, String entryName)
      throws Exception {
    File targetFile = tempDir.resolve("container.tar").toFile();
    Path path = targetFile.toPath();
    try (TarArchiveOutputStream archive = new TarArchiveOutputStream(packer.compress(newOutputStream(path)))) {
      archive.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
      Archiver.includeFile(file, entryName, archive);
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

    assertTrue(Files.exists(exampleFile),
        "example file is missing after pack/unpackContainerData: " +
            exampleFile);

    try (InputStream testFile =
             newInputStream(exampleFile)) {
      List<String> strings = IOUtils.readLines(testFile, UTF_8);
      assertEquals(1, strings.size());
      assertEquals(content, strings.get(0));
    }
  }

}

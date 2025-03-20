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

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V3;

import com.google.common.annotations.VisibleForTesting;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerPacker;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.apache.hadoop.ozone.container.replication.CopyContainerCompression;

/**
 * Compress/uncompress KeyValueContainer data to a tar archive.
 */
public class TarContainerPacker
    implements ContainerPacker<KeyValueContainerData> {

  static final String CHUNKS_DIR_NAME = OzoneConsts.STORAGE_DIR_CHUNKS;

  static final String DB_DIR_NAME = "db";

  static final String CONTAINER_FILE_NAME = "container.yaml";

  private final CopyContainerCompression compression;

  public TarContainerPacker(CopyContainerCompression compression) {
    this.compression = compression;
  }

  /**
   * Given an input stream (tar file) extract the data to the specified
   * directories.
   *
   * @param container container which defines the destination structure.
   * @param input the input stream.
   */
  @Override
  public byte[] unpackContainerData(Container<KeyValueContainerData> container,
      InputStream input, Path tmpDir, Path destContainerDir)
      throws IOException {
    KeyValueContainerData containerData = container.getContainerData();
    long containerId = containerData.getContainerID();

    Path containerUntarDir = tmpDir.resolve(String.valueOf(containerId));
    if (containerUntarDir.toFile().exists()) {
      FileUtils.deleteDirectory(containerUntarDir.toFile());
    }

    Path dbRoot = getDbPath(containerUntarDir, containerData);
    Path chunksRoot = getChunkPath(containerUntarDir, containerData);
    byte[] descriptorFileContent = innerUnpack(input, dbRoot, chunksRoot);

    if (!Files.exists(destContainerDir)) {
      Files.createDirectories(destContainerDir);
    }
    if (FileUtils.isEmptyDirectory(destContainerDir.toFile())) {
      Files.move(containerUntarDir, destContainerDir,
              StandardCopyOption.ATOMIC_MOVE,
              StandardCopyOption.REPLACE_EXISTING);
    } else {
      String errorMessage = "Container " + containerId +
          " unpack failed because ContainerFile " +
          destContainerDir.toAbsolutePath() + " already exists";
      throw new StorageContainerException(errorMessage,
          CONTAINER_ALREADY_EXISTS);
    }
    return descriptorFileContent;
  }

  private void extractEntry(ArchiveEntry entry, InputStream input, long size,
      Path ancestor, Path path) throws IOException {
    HddsUtils.validatePath(path, ancestor);

    if (entry.isDirectory()) {
      Files.createDirectories(path);
    } else {
      Path parent = path.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }

      try (OutputStream fileOutput = Files.newOutputStream(path);
           OutputStream output = new BufferedOutputStream(fileOutput)) {
        int bufferSize = 1024;
        byte[] buffer = new byte[bufferSize + 1];
        long remaining = size;
        while (remaining > 0) {
          int len = (int) Math.min(remaining, bufferSize);
          int read = input.read(buffer, 0, len);
          if (read >= 0) {
            remaining -= read;
            output.write(buffer, 0, read);
          } else {
            remaining = 0;
          }
        }
      }
    }
  }

  /**
   * Given a containerData include all the required container data/metadata
   * in a tar file.
   *
   * @param container Container to archive (data + metadata).
   * @param output   Destination tar file/stream.
   */
  @Override
  public void pack(Container<KeyValueContainerData> container,
      OutputStream output)
      throws IOException {

    KeyValueContainerData containerData = container.getContainerData();

    try (ArchiveOutputStream<TarArchiveEntry> archiveOutput = tar(compress(output))) {
      includeFile(container.getContainerFile(), CONTAINER_FILE_NAME,
          archiveOutput);

      includePath(getDbPath(containerData), DB_DIR_NAME,
          archiveOutput);

      includePath(Paths.get(containerData.getChunksPath()), CHUNKS_DIR_NAME,
          archiveOutput);
    }
  }

  @Override
  public byte[] unpackContainerDescriptor(InputStream input)
      throws IOException {
    try (ArchiveInputStream<TarArchiveEntry> archiveInput = untar(decompress(input))) {

      ArchiveEntry entry = archiveInput.getNextEntry();
      while (entry != null) {
        String name = entry.getName();
        if (CONTAINER_FILE_NAME.equals(name)) {
          return readEntry(archiveInput, entry.getSize());
        }
        entry = archiveInput.getNextEntry();
      }
    }

    throw new IOException(
        "Container descriptor is missing from the container archive.");
  }

  public static Path getDbPath(KeyValueContainerData containerData) {
    if (containerData.hasSchema(SCHEMA_V3)) {
      return DatanodeStoreSchemaThreeImpl.getDumpDir(
          new File(containerData.getMetadataPath())).toPath();
    } else {
      return containerData.getDbFile().toPath();
    }
  }

  public static Path getDbPath(Path baseDir,
      KeyValueContainerData containerData) {
    if (baseDir.toAbsolutePath().toString().equals(
        containerData.getContainerPath())) {
      return getDbPath(containerData);
    }
    Path containerPath = Paths.get(containerData.getContainerPath());
    Path dbPath = Paths.get(containerData.getDbFile().getPath());
    Path relativePath = containerPath.relativize(dbPath);

    if (containerData.hasSchema(SCHEMA_V3)) {
      Path metadataDir = KeyValueContainerLocationUtil.getContainerMetaDataPath(
          baseDir.toString()).toPath();
      return DatanodeStoreSchemaThreeImpl.getDumpDir(metadataDir.toFile())
          .toPath();
    } else {
      return baseDir.resolve(relativePath);
    }
  }

  public static Path getChunkPath(Path baseDir,
      KeyValueContainerData containerData) {
    return KeyValueContainerLocationUtil.getChunksLocationPath(baseDir.toString()).toPath();
  }

  private byte[] readEntry(InputStream input, final long size)
      throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    int bufferSize = 1024;
    byte[] buffer = new byte[bufferSize + 1];
    long remaining = size;
    while (remaining > 0) {
      int len = (int) Math.min(remaining, bufferSize);
      int read = input.read(buffer, 0, len);
      remaining -= read;
      output.write(buffer, 0, read);
    }
    return output.toByteArray();
  }

  private void includePath(Path dir, String subdir,
      ArchiveOutputStream<TarArchiveEntry> archiveOutput) throws IOException {

    // Add a directory entry before adding files, in case the directory is
    // empty.
    TarArchiveEntry entry = archiveOutput.createArchiveEntry(dir.toFile(), subdir);
    archiveOutput.putArchiveEntry(entry);
    archiveOutput.closeArchiveEntry();

    // Add files in the directory.
    try (Stream<Path> dirEntries = Files.list(dir)) {
      for (Path path : dirEntries.collect(toList())) {
        String entryName = subdir + "/" + path.getFileName();
        includeFile(path.toFile(), entryName, archiveOutput);
      }
    }
  }

  static void includeFile(File file, String entryName,
      ArchiveOutputStream<TarArchiveEntry> archiveOutput) throws IOException {
    TarArchiveEntry entry = archiveOutput.createArchiveEntry(file, entryName);
    archiveOutput.putArchiveEntry(entry);
    try (InputStream input = Files.newInputStream(file.toPath())) {
      IOUtils.copy(input, archiveOutput);
    }
    archiveOutput.closeArchiveEntry();
  }

  private static ArchiveInputStream<TarArchiveEntry> untar(InputStream input) {
    return new TarArchiveInputStream(input);
  }

  private static ArchiveOutputStream<TarArchiveEntry> tar(OutputStream output) {
    TarArchiveOutputStream os = new TarArchiveOutputStream(output);
    os.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
    return os;
  }

  @VisibleForTesting
  InputStream decompress(InputStream input) throws IOException {
    return compression.wrap(input);
  }

  @VisibleForTesting
  OutputStream compress(OutputStream output) throws IOException {
    return compression.wrap(output);
  }

  private byte[] innerUnpack(InputStream input, Path dbRoot, Path chunksRoot)
      throws IOException {
    byte[] descriptorFileContent = null;
    try (ArchiveInputStream<TarArchiveEntry> archiveInput = untar(decompress(input))) {
      ArchiveEntry entry = archiveInput.getNextEntry();
      while (entry != null) {
        String name = entry.getName();
        long size = entry.getSize();
        if (name.startsWith(DB_DIR_NAME + "/")) {
          Path destinationPath = dbRoot
              .resolve(name.substring(DB_DIR_NAME.length() + 1));
          extractEntry(entry, archiveInput, size, dbRoot,
              destinationPath);
        } else if (name.startsWith(CHUNKS_DIR_NAME + "/")) {
          Path destinationPath = chunksRoot
              .resolve(name.substring(CHUNKS_DIR_NAME.length() + 1));
          extractEntry(entry, archiveInput, size, chunksRoot,
              destinationPath);
        } else if (CONTAINER_FILE_NAME.equals(name)) {
          //Don't do anything. Container file should be unpacked in a
          //separated step by unpackContainerDescriptor call.
          descriptorFileContent = readEntry(archiveInput, size);
        } else {
          throw new IllegalArgumentException(
              "Unknown entry in the tar file: " + name);
        }
        entry = archiveInput.getNextEntry();
      }
      return descriptorFileContent;
    }
  }
}

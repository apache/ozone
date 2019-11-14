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

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerPacker;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;

import static java.util.stream.Collectors.toList;

/**
 * Compress/uncompress KeyValueContainer data to a tar.gz archive.
 */
public class TarContainerPacker
    implements ContainerPacker<KeyValueContainerData> {

  static final String CHUNKS_DIR_NAME = OzoneConsts.STORAGE_DIR_CHUNKS;

  static final String DB_DIR_NAME = "db";

  private static final String CONTAINER_FILE_NAME = "container.yaml";

  /**
   * Given an input stream (tar file) extract the data to the specified
   * directories.
   *
   * @param container container which defines the destination structure.
   * @param input the input stream.
   */
  @Override
  public byte[] unpackContainerData(Container<KeyValueContainerData> container,
      InputStream input)
      throws IOException {
    byte[] descriptorFileContent = null;
    KeyValueContainerData containerData = container.getContainerData();
    Path dbRoot = containerData.getDbFile().toPath();
    Path chunksRoot = Paths.get(containerData.getChunksPath());

    try (InputStream decompressed = decompress(input);
         ArchiveInputStream archiveInput = untar(decompressed)) {

      ArchiveEntry entry = archiveInput.getNextEntry();
      while (entry != null) {
        String name = entry.getName();
        long size = entry.getSize();
        if (name.startsWith(DB_DIR_NAME + "/")) {
          Path destinationPath = dbRoot
              .resolve(name.substring(DB_DIR_NAME.length() + 1));
          extractEntry(archiveInput, size, dbRoot, destinationPath);
        } else if (name.startsWith(CHUNKS_DIR_NAME + "/")) {
          Path destinationPath = chunksRoot
              .resolve(name.substring(CHUNKS_DIR_NAME.length() + 1));
          extractEntry(archiveInput, size, chunksRoot, destinationPath);
        } else if (CONTAINER_FILE_NAME.equals(name)) {
          //Don't do anything. Container file should be unpacked in a
          //separated step by unpackContainerDescriptor call.
          descriptorFileContent = readEntry(archiveInput, size);
        } else {
          throw new IllegalArgumentException(
              "Unknown entry in the tar file: " + "" + name);
        }
        entry = archiveInput.getNextEntry();
      }
      return descriptorFileContent;

    } catch (CompressorException e) {
      throw new IOException(
          "Can't uncompress the given container: " + container
              .getContainerData().getContainerID(),
          e);
    }
  }

  private void extractEntry(InputStream input, long size,
                            Path ancestor, Path path) throws IOException {
    HddsUtils.validatePath(path, ancestor);
    Path parent = path.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }

    try (OutputStream fileOutput = new FileOutputStream(path.toFile());
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

    try (OutputStream compressed = compress(output);
         ArchiveOutputStream archiveOutput = tar(compressed)) {

      includePath(containerData.getDbFile().toPath(), DB_DIR_NAME,
          archiveOutput);

      includePath(Paths.get(containerData.getChunksPath()), CHUNKS_DIR_NAME,
          archiveOutput);

      includeFile(container.getContainerFile(), CONTAINER_FILE_NAME,
          archiveOutput);
    } catch (CompressorException e) {
      throw new IOException(
          "Can't compress the container: " + containerData.getContainerID(),
          e);
    }
  }

  @Override
  public byte[] unpackContainerDescriptor(InputStream input)
      throws IOException {
    try (InputStream decompressed = decompress(input);
         ArchiveInputStream archiveInput = untar(decompressed)) {

      ArchiveEntry entry = archiveInput.getNextEntry();
      while (entry != null) {
        String name = entry.getName();
        if (CONTAINER_FILE_NAME.equals(name)) {
          return readEntry(archiveInput, entry.getSize());
        }
        entry = archiveInput.getNextEntry();
      }
    } catch (CompressorException e) {
      throw new IOException(
          "Can't read the container descriptor from the container archive",
          e);
    }

    throw new IOException(
        "Container descriptor is missing from the container archive.");
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
      ArchiveOutputStream archiveOutput) throws IOException {

    try (Stream<Path> dirEntries = Files.list(dir)) {
      for (Path path : dirEntries.collect(toList())) {
        String entryName = subdir + "/" + path.getFileName();
        includeFile(path.toFile(), entryName, archiveOutput);
      }
    }
  }

  static void includeFile(File file, String entryName,
      ArchiveOutputStream archiveOutput) throws IOException {
    ArchiveEntry entry = archiveOutput.createArchiveEntry(file, entryName);
    archiveOutput.putArchiveEntry(entry);
    try (InputStream input = new FileInputStream(file)) {
      IOUtils.copy(input, archiveOutput);
    }
    archiveOutput.closeArchiveEntry();
  }

  private static ArchiveInputStream untar(InputStream input) {
    return new TarArchiveInputStream(input);
  }

  private static ArchiveOutputStream tar(OutputStream output) {
    return new TarArchiveOutputStream(output);
  }

  private static InputStream decompress(InputStream input)
      throws CompressorException {
    return new CompressorStreamFactory()
        .createCompressorInputStream(CompressorStreamFactory.GZIP, input);
  }

  private static OutputStream compress(OutputStream output)
      throws CompressorException {
    return new CompressorStreamFactory()
        .createCompressorOutputStream(CompressorStreamFactory.GZIP, output);
  }

}

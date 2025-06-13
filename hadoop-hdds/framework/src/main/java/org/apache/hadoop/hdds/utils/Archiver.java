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

package org.apache.hadoop.hdds.utils;

import static java.util.stream.Collectors.toList;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.HddsUtils;

/** Create and extract archives. */
public final class Archiver {

  private Archiver() {
    // no instances (for now)
  }

  /** Create tarball including contents of {@code from}. */
  public static void create(File tarFile, Path from) throws IOException {
    try (ArchiveOutputStream<TarArchiveEntry> out = tar(Files.newOutputStream(tarFile.toPath()))) {
      includePath(from, "", out);
    }
  }

  /** Extract {@code tarFile} to {@code dir}. */
  public static void extract(File tarFile, Path dir) throws IOException {
    Files.createDirectories(dir);
    String parent = dir.toString();
    try (ArchiveInputStream<TarArchiveEntry> in = untar(Files.newInputStream(tarFile.toPath()))) {
      TarArchiveEntry entry;
      while ((entry = in.getNextEntry()) != null) {
        Path path = Paths.get(parent, entry.getName());
        extractEntry(entry, in, entry.getSize(), dir, path);
      }
    }
  }

  public static byte[] readEntry(InputStream input, final long size)
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

  public static void includePath(Path dir, String subdir,
      ArchiveOutputStream<TarArchiveEntry> archiveOutput) throws IOException {

    // Add a directory entry before adding files, in case the directory is
    // empty.
    TarArchiveEntry entry = archiveOutput.createArchiveEntry(dir.toFile(), subdir);
    archiveOutput.putArchiveEntry(entry);
    archiveOutput.closeArchiveEntry();

    // Add files in the directory.
    try (Stream<Path> dirEntries = Files.list(dir)) {
      for (Path path : dirEntries.collect(toList())) {
        File file = path.toFile();
        String entryName = subdir + "/" + path.getFileName();
        if (file.isDirectory()) {
          includePath(path, entryName, archiveOutput);
        } else {
          includeFile(file, entryName, archiveOutput);
        }
      }
    }
  }

  public static long includeFile(File file, String entryName,
      ArchiveOutputStream<TarArchiveEntry> archiveOutput) throws IOException {
    final long bytes;
    TarArchiveEntry entry = archiveOutput.createArchiveEntry(file, entryName);
    archiveOutput.putArchiveEntry(entry);
    try (InputStream input = Files.newInputStream(file.toPath())) {
      bytes = IOUtils.copyLarge(input, archiveOutput);
    }
    archiveOutput.closeArchiveEntry();
    return bytes;
  }

  public static void extractEntry(ArchiveEntry entry, InputStream input, long size,
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

  public static ArchiveInputStream<TarArchiveEntry> untar(InputStream input) {
    return new TarArchiveInputStream(input);
  }

  public static ArchiveOutputStream<TarArchiveEntry> tar(OutputStream output) {
    TarArchiveOutputStream os = new TarArchiveOutputStream(output);
    os.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
    os.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
    return os;
  }

}

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
import java.nio.file.NoSuchFileException;
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
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Create and extract archives. */
public final class Archiver {

  static final int MIN_BUFFER_SIZE = 8 * (int) OzoneConsts.KB; // same as IOUtils.DEFAULT_BUFFER_SIZE
  static final int MAX_BUFFER_SIZE = (int) OzoneConsts.MB;
  private static final Logger LOG = LoggerFactory.getLogger(Archiver.class);

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
    IOUtils.copy(input, output, getBufferSize(size));
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
      bytes = IOUtils.copy(input, archiveOutput, getBufferSize(file.length()));
    }
    archiveOutput.closeArchiveEntry();
    return bytes;
  }

  /**
   * Creates a hard link to the specified file in the provided temporary directory,
   * adds the linked file as an entry to the archive with the given entry name, writes
   * its contents to the archive output, and then deletes the temporary hard link.
   * <p>
   * This approach avoids altering the original file and works around limitations
   * of certain archiving libraries that may require the source file to be present
   * in a specific location or have a specific name. Any errors during the hardlink
   * creation or archiving process are logged.
   * </p>
   *
   * @param file         the file to be included in the archive
   * @param entryName    the name/path under which the file should appear in the archive
   * @param archiveOutput the output stream for the archive (e.g., tar)
   * @param tmpDir       the temporary directory in which to create the hard link
   * @return number of bytes copied to the archive for this file
   * @throws IOException if an I/O error occurs other than hardlink creation failure
   */
  public static long linkAndIncludeFile(File file, String entryName,
      ArchiveOutputStream<TarArchiveEntry> archiveOutput, Path tmpDir) throws IOException {
    File link = tmpDir.resolve(entryName).toFile();
    long bytes = 0;
    try {
      try {
        Files.createLink(link.toPath(), file.toPath());
      } catch (NoSuchFileException noSuchFileException){
        LOG.warn("Failed to include file {} in the tarball as the file doesn't exist", file.toPath());
        return  0;
      }
      TarArchiveEntry entry = archiveOutput.createArchiveEntry(link, entryName);
      archiveOutput.putArchiveEntry(entry);
      try (InputStream input = Files.newInputStream(link.toPath())) {
        bytes = IOUtils.copyLarge(input, archiveOutput);
      }
      archiveOutput.closeArchiveEntry();
    } catch (IOException ioe) {
      LOG.error("Couldn't create hardlink for file {} while including it in tarball.",
          file.getAbsolutePath(), ioe);
      throw ioe;
    } finally {
      Files.deleteIfExists(link.toPath());
    }
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
        IOUtils.copy(input, output, getBufferSize(size));
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

  static int getBufferSize(long fileSize) {
    return Math.toIntExact(Math.min(MAX_BUFFER_SIZE, Math.max(fileSize, MIN_BUFFER_SIZE)));
  }

}

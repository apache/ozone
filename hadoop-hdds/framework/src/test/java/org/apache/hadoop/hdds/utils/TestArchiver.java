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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

/** Test {@link Archiver}. */
class TestArchiver {

  @ParameterizedTest
  @ValueSource(longs = { 0, 1, Archiver.MIN_BUFFER_SIZE - 1 })
  void bufferSizeBelowMinimum(long fileSize) {
    assertThat(Archiver.getBufferSize(fileSize))
        .isEqualTo(Archiver.MIN_BUFFER_SIZE);
  }

  @ParameterizedTest
  @ValueSource(longs = { Archiver.MIN_BUFFER_SIZE, Archiver.MAX_BUFFER_SIZE })
  void bufferSizeBetweenLimits(long fileSize) {
    assertThat(Archiver.getBufferSize(fileSize))
        .isEqualTo(fileSize);
  }

  @ParameterizedTest
  @ValueSource(longs = { Archiver.MAX_BUFFER_SIZE + 1, Integer.MAX_VALUE, Long.MAX_VALUE })
  void bufferSizeAboveMaximum(long fileSize) {
    assertThat(Archiver.getBufferSize(fileSize))
        .isEqualTo(Archiver.MAX_BUFFER_SIZE);
  }

  @Test
  void testLinkAndIncludeFileSuccessfulHardLink() throws IOException {
    Path tmpDir = Files.createTempDirectory("archiver-test");
    File tempFile = File.createTempFile("test-file", ".txt");
    String entryName = "test-entry";
    Files.write(tempFile.toPath(), "Test Content".getBytes(StandardCharsets.UTF_8));

    TarArchiveOutputStream mockArchiveOutput = mock(TarArchiveOutputStream.class);
    TarArchiveEntry mockEntry = new TarArchiveEntry(entryName);
    AtomicBoolean isHardLinkCreated = new AtomicBoolean(false);
    when(mockArchiveOutput.createArchiveEntry(any(File.class), eq(entryName)))
        .thenAnswer(invocation -> {
          File linkFile = invocation.getArgument(0);
          isHardLinkCreated.set(Files.isSameFile(tempFile.toPath(), linkFile.toPath()));
          return mockEntry;
        });

    // Call method under test
    long bytesCopied = Archiver.linkAndIncludeFile(tempFile, entryName, mockArchiveOutput, tmpDir);
    assertEquals(Files.size(tempFile.toPath()), bytesCopied);
    // Verify archive interactions
    verify(mockArchiveOutput, times(1)).putArchiveEntry(mockEntry);
    verify(mockArchiveOutput, times(1)).closeArchiveEntry();
    assertTrue(isHardLinkCreated.get());
    assertFalse(Files.exists(tmpDir.resolve(entryName)));
    // Cleanup
    assertTrue(tempFile.delete());
    Files.deleteIfExists(tmpDir);
  }

  @Test
  void testLinkAndIncludeFileFailedHardLink() throws IOException {
    Path tmpDir = Files.createTempDirectory("archiver-test");
    File tempFile = File.createTempFile("test-file", ".txt");
    String entryName = "test-entry";
    Files.write(tempFile.toPath(), "Test Content".getBytes(StandardCharsets.UTF_8));

    TarArchiveOutputStream mockArchiveOutput =
        mock(TarArchiveOutputStream.class);
    TarArchiveEntry mockEntry = new TarArchiveEntry("test-entry");
    AtomicBoolean isHardLinkCreated = new AtomicBoolean(false);
    when(mockArchiveOutput.createArchiveEntry(any(File.class), eq(entryName)))
        .thenAnswer(invocation -> {
          File linkFile = invocation.getArgument(0);
          isHardLinkCreated.set(Files.isSameFile(tempFile.toPath(), linkFile.toPath()));
          return mockEntry;
        });

    // Mock static Files.createLink to throw IOException
    try (MockedStatic<Files> mockedFiles = mockStatic(Files.class, CALLS_REAL_METHODS)) {
      Path linkPath = tmpDir.resolve(entryName);
      String errorMessage = "Failed to create hardlink";
      mockedFiles.when(() -> Files.createLink(linkPath, tempFile.toPath()))
          .thenThrow(new IOException(errorMessage));

      IOException thrown = assertThrows(IOException.class, () ->
          Archiver.linkAndIncludeFile(tempFile, entryName, mockArchiveOutput, tmpDir)
      );

      assertTrue(thrown.getMessage().contains(errorMessage));
    }
    assertFalse(isHardLinkCreated.get());

    assertTrue(tempFile.delete());
    Files.deleteIfExists(tmpDir);
  }

}

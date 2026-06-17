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

package org.apache.hadoop.ozone.container.ozoneimpl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystemException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScanError.FailureType;
import org.junit.jupiter.api.Test;

/** 
 * Unit tests for {@link ScanTransientIOUtil}.
 */
public class TestScanTransientIOUtil {

  @Test
  public void detectsTooManyOpenFilesInFileSystemException() {
    assertTrue(ScanTransientIOUtil.isTooManyOpenFiles(new FileSystemException(null, null, "Too many open files")));
  }

  @Test
  public void detectsTooManyOpenFilesInFileNotFoundExceptionMessage() {
    String msg = "/data/container/metadata/16341719.container (Too many open files)";
    assertTrue(ScanTransientIOUtil.isTooManyOpenFiles(new FileNotFoundException(msg)));
  }

  @Test
  public void detectsTooManyOpenFilesInMessageCauseChain() {
    IOException throwable = new IOException("Too many open files");
    assertTrue(ScanTransientIOUtil.isTooManyOpenFiles(new IOException(throwable)));
  }

  @Test
  public void rejectsUnrelatedIOException() {
    assertFalse(ScanTransientIOUtil.isTooManyOpenFiles(new IOException("disk full")));
  }

  @Test
  public void scanErrorsOnlyTooManyOpenFilesReturnsTrue() {
    IOException ex = new IOException("Too many open files");
    MetadataScanResult scanResult = MetadataScanResult.fromErrors(Collections.singletonList(
        new ContainerScanError(FailureType.CORRUPT_CONTAINER_FILE, new File("."), ex)));
    assertTrue(ScanTransientIOUtil.scanErrorsAreOnlyTooManyOpenFiles(scanResult));
  }

  @Test
  public void scanErrorsMixedReturnsFalse() {
    IOException ioException = new IOException("Too many open files");
    FileNotFoundException fileNotFoundException = new FileNotFoundException("missing");
    MetadataScanResult scanResult = MetadataScanResult.fromErrors(Arrays.asList(
        new ContainerScanError(FailureType.CORRUPT_CHUNK, new File("."), ioException),
        new ContainerScanError(FailureType.MISSING_CONTAINER_FILE, new File("."), fileNotFoundException)));
    assertFalse(ScanTransientIOUtil.scanErrorsAreOnlyTooManyOpenFiles(scanResult));
  }

  @Test
  public void emptyScanResult() {
    assertFalse(ScanTransientIOUtil.scanErrorsAreOnlyTooManyOpenFiles(
        MetadataScanResult.fromErrors(Collections.emptyList())));
  }
}

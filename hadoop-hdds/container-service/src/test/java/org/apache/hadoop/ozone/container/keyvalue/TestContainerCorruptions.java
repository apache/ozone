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

import static org.apache.hadoop.ozone.container.ContainerTestHelper.corruptFile;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.truncateFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScanError;
import org.apache.ozone.test.GenericTestUtils;

/**
 * Represents a type of container corruption that can be injected into a container for testing.
 * Currently this class only supports file per block layout.
 */
public enum TestContainerCorruptions {
  MISSING_CHUNKS_DIR((container, blockID) -> {
    File chunksDir = new File(container.getContainerData().getContainerPath(),
        "chunks");
    try {
      FileUtils.deleteDirectory(chunksDir);
    } catch (IOException ex) {
      // Fail the test.
      throw new UncheckedIOException(ex);
    }
    assertFalse(chunksDir.exists());
  }, ContainerScanError.FailureType.MISSING_CHUNKS_DIR),

  MISSING_METADATA_DIR((container, blockID) -> {
    File metadataDir =
        new File(container.getContainerData().getContainerPath(),
            "metadata");
    try {
      FileUtils.deleteDirectory(metadataDir);
    } catch (IOException ex) {
      // Fail the test.
      throw new UncheckedIOException(ex);
    }
    assertFalse(metadataDir.exists());
  }, ContainerScanError.FailureType.MISSING_METADATA_DIR),

  MISSING_CONTAINER_FILE((container, blockID) -> {
    File containerFile = container.getContainerFile();
    assertTrue(containerFile.delete());
    assertFalse(containerFile.exists());
  }, ContainerScanError.FailureType.MISSING_CONTAINER_FILE),

  MISSING_CONTAINER_DIR((container, blockID) -> {
    File containerDir =
        new File(container.getContainerData().getContainerPath());
    try {
      FileUtils.deleteDirectory(containerDir);
    } catch (IOException ex) {
      // Fail the test.
      throw new UncheckedIOException(ex);
    }
    assertFalse(containerDir.exists());
  }, ContainerScanError.FailureType.MISSING_CONTAINER_DIR),

  MISSING_BLOCK((container, blockID) -> {
    File blockFile = getBlock(container, blockID);
    assertTrue(blockFile.delete());
  }, ContainerScanError.FailureType.MISSING_DATA_FILE),

  CORRUPT_CONTAINER_FILE((container, blockID) -> {
    File containerFile = container.getContainerFile();
    corruptFile(containerFile);
  }, ContainerScanError.FailureType.CORRUPT_CONTAINER_FILE),

  TRUNCATED_CONTAINER_FILE((container, blockID) -> {
    File containerFile = container.getContainerFile();
    truncateFile(containerFile);
  }, ContainerScanError.FailureType.CORRUPT_CONTAINER_FILE),

  CORRUPT_BLOCK((container, blockID) -> {
    File blockFile = getBlock(container, blockID);
    corruptFile(blockFile);
  }, ContainerScanError.FailureType.CORRUPT_CHUNK),

  TRUNCATED_BLOCK((container, blockID) -> {
    File blockFile = getBlock(container, blockID);
    truncateFile(blockFile);
  },
      // This test completely removes all content from the block file. The scanner will see this as all the chunks in
      // the block missing, hence MISSING_CHUNK instead of INCONSISTENT_CHUNK_LENGTH.
      ContainerScanError.FailureType.MISSING_CHUNK);

  private final BiConsumer<Container<?>, Long> corruption;
  private final ContainerScanError.FailureType expectedResult;

  TestContainerCorruptions(BiConsumer<Container<?>, Long> corruption, ContainerScanError.FailureType expectedResult) {
    this.corruption = corruption;
    this.expectedResult = expectedResult;

  }

  public void applyTo(Container<?> container) {
    corruption.accept(container, -1L);
  }

  public void applyTo(Container<?> container, long blockID) {
    corruption.accept(container, blockID);
  }

  /**
   * Check that the correct corruption type was written to the container log for the provided container.
   */
  public void assertLogged(long containerID, int numErrors, GenericTestUtils.LogCapturer logCapturer) {
    // Enable multiline regex mode with "(?m)". This allows ^ to check for the start of a line in a multiline string.
    // The log will have captured lines from all previous tests as well since we re-use the same cluster.
    Pattern logLine = Pattern.compile("(?m)^ID=" + containerID + ".*" + " Container has " + numErrors +
        " error.*" + expectedResult.toString());
    assertThat(logCapturer.getOutput()).containsPattern(logLine);
  }

  /**
   * Check that the correct corruption type was written to the container log for the provided container.
   */
  public void assertLogged(long containerID, GenericTestUtils.LogCapturer logCapturer) {
    // Enable multiline regex mode with "(?m)". This allows ^ to check for the start of a line in a multiline string.
    // The log will have captured lines from all previous tests as well since we re-use the same cluster.
    Pattern logLine = Pattern.compile("(?m)^ID=" + containerID + ".*" + " Container has .*error.*" +
        expectedResult.toString());
    assertThat(logCapturer.getOutput()).containsPattern(logLine);
  }

  public ContainerScanError.FailureType getExpectedResult() {
    return expectedResult;
  }

  /**
   * Get all container corruption types as parameters for junit 4
   * parameterized tests, except the ones specified.
   */
  public static Set<TestContainerCorruptions> getAllParamsExcept(
      TestContainerCorruptions... exclude) {
    Set<TestContainerCorruptions> includeSet =
        EnumSet.allOf(TestContainerCorruptions.class);
    Arrays.asList(exclude).forEach(includeSet::remove);
    return includeSet;
  }

  public static File getBlock(Container<?> container, long blockID) {
    File blockFile;
    File chunksDir = new File(container.getContainerData().getContainerPath(),
        "chunks");
    // Negative values are an internal placeholder to get the first block in a container.
    if (blockID < 0) {
      File[] blockFiles = chunksDir.listFiles((dir, name) -> name.endsWith(".block"));
      assertNotNull(blockFiles);
      assertTrue(blockFiles.length > 0);
      blockFile = blockFiles[0];
    } else {
      // Get the block by ID.
      blockFile = new File(chunksDir, blockID + ".block");
    }
    assertTrue(blockFile.exists());
    return blockFile;
  }
}

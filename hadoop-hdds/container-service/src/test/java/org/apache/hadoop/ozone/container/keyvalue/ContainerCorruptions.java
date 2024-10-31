package org.apache.hadoop.ozone.container.keyvalue;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScanError;
import org.apache.ozone.test.GenericTestUtils;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Represents a type of container corruption that can be injected into the
 * test.
 */
public enum ContainerCorruptions {
  MISSING_CHUNKS_DIR((container, index) -> {
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

  MISSING_METADATA_DIR((container, index) -> {
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

  MISSING_CONTAINER_FILE((container, index) -> {
    File containerFile = container.getContainerFile();
    assertTrue(containerFile.delete());
    assertFalse(containerFile.exists());
  }, ContainerScanError.FailureType.MISSING_CONTAINER_FILE),

  MISSING_CONTAINER_DIR((container, index) -> {
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

  MISSING_BLOCK((container, index) -> {
    File blockFile = getBlockAtIndex(container, index);
    assertTrue(blockFile.delete());
  }, ContainerScanError.FailureType.MISSING_CHUNK_FILE),

  CORRUPT_CONTAINER_FILE((container, index) -> {
    File containerFile = container.getContainerFile();
    corruptFile(containerFile);
  }, ContainerScanError.FailureType.CORRUPT_CONTAINER_FILE),

  TRUNCATED_CONTAINER_FILE((container, index) -> {
    File containerFile = container.getContainerFile();
    truncateFile(containerFile);
  }, ContainerScanError.FailureType.CORRUPT_CONTAINER_FILE),

  CORRUPT_BLOCK((container, index) -> {
    File blockFile = getBlockAtIndex(container, index);
    corruptFile(blockFile);
  }, ContainerScanError.FailureType.CORRUPT_CHUNK),

  TRUNCATED_BLOCK((container, index) -> {
    File blockFile = getBlockAtIndex(container, index);
    truncateFile(blockFile);
  }, ContainerScanError.FailureType.INCONSISTENT_CHUNK_LENGTH);

  private final BiConsumer<Container<?>, Integer> corruption;
  private final ContainerScanError.FailureType expectedResult;

  ContainerCorruptions(BiConsumer<Container<?>, Integer> corruption, ContainerScanError.FailureType expectedResult) {
    this.corruption = corruption;
    this.expectedResult = expectedResult;

  }

  public void applyTo(Container<?> container) {
    corruption.accept(container,0);
  }

  public void applyTo(Container<?> container, int blockIndex) {
    corruption.accept(container, blockIndex);
  }

  /**
   * Check that the correct corruption type was written to the container log for the provided container.
   */
  public void assertLogged(long containerID, int numErrors, GenericTestUtils.LogCapturer logCapturer) {
    // Enable multiline regex mode with "(?m)". This allows ^ to check for the start of a line in a multiline string.
    // The log will have captured lines from all previous tests as well since we re-use the same cluster.
    Pattern logLine = Pattern.compile("(?m)^ID=" + containerID + ".*" + " Scan result has " + numErrors +
        " error.*" + expectedResult.toString());
    assertThat(logCapturer.getOutput()).containsPattern(logLine);
  }

  public ContainerScanError.FailureType getExpectedResult() {
    return expectedResult;
  }

  /**
   * Get all container corruption types as parameters for junit 4
   * parameterized tests, except the ones specified.
   */
  public static Set<ContainerCorruptions> getAllParamsExcept(
      ContainerCorruptions... exclude) {
    Set<ContainerCorruptions> includeSet =
        EnumSet.allOf(ContainerCorruptions.class);
    Arrays.asList(exclude).forEach(includeSet::remove);
    return includeSet;
  }

  /**
   * Overwrite the file with random bytes.
   */
  private static void corruptFile(File file) {
    try {
      final int length = (int) file.length();

      Path path = file.toPath();
      final byte[] original = IOUtils.readFully(Files.newInputStream(path), length);

      // Corrupt the last byte of the last chunk. This should map to a single error from the scanner.
      final byte[] corruptedBytes = Arrays.copyOf(original, length);
      corruptedBytes[length - 1] = (byte) (original[length - 1] << 1);

      Files.write(path, corruptedBytes,
          StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);

      assertThat(IOUtils.readFully(Files.newInputStream(path), length))
          .isEqualTo(corruptedBytes)
          .isNotEqualTo(original);
    } catch (IOException ex) {
      // Fail the test.
      throw new UncheckedIOException(ex);
    }
  }

  private static File getBlockAtIndex(Container<?> container, int index) {
    File chunksDir = new File(container.getContainerData().getContainerPath(),
        "chunks");
    File blockFile = Arrays.stream(Objects.requireNonNull(
            chunksDir.listFiles((dir, name) -> name.endsWith(".block"))))
        .sorted(Comparator.comparing(File::getName))
        .collect(Collectors.toList()).get(index);
    assertTrue(blockFile.exists());
    return blockFile;
  }

  /**
   * Truncate the file to 0 bytes in length.
   */
  private static void truncateFile(File file) {
    try {
      Files.write(file.toPath(), new byte[0],
          StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);

      assertEquals(0, file.length());
    } catch (IOException ex) {
      // Fail the test.
      throw new UncheckedIOException(ex);
    }
  }
}

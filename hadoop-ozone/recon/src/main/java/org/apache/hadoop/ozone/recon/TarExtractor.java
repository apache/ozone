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

package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.ozone.recon.ReconConstants.STAGING;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for extracting files from a TAR archive using a multi-threaded approach.
 *
 * <p>This class utilizes an {@link ExecutorService} to extract files concurrently,
 * improving performance when handling large TAR archives.</p>
 *
 * <p>Usage:</p>
 * <pre>
 * {@code
 * TarExtractor tarExtractor = new TarExtractor(10, "extractor-thread");
 * tarExtractor.extractTar(inputStream, outputPath);
 * tarExtractor.shutdown();
 * }
 * </pre>
 */
public class TarExtractor {
  private static final Logger LOG =
      LoggerFactory.getLogger(TarExtractor.class);

  private final AtomicBoolean executorServiceStarted = new AtomicBoolean(false);
  private int threadPoolSize;
  private ExecutorService executor;
  private ThreadFactory threadFactory;

  public TarExtractor(int threadPoolSize, String threadNamePrefix) {
    this.threadPoolSize = threadPoolSize;
    this.threadFactory =
        new ThreadFactoryBuilder().setNameFormat("FetchOMDBTar-%d" + threadNamePrefix)
            .build();
  }

  public void extractTar(InputStream tarStream, Path outputDir)
      throws IOException, InterruptedException, ExecutionException {
    String stagingDirName = STAGING + UUID.randomUUID();
    Path parentDir = outputDir.getParent();
    if (parentDir == null) {
      parentDir = outputDir; // Handle null parent case
    }
    Path stagingDir = parentDir.resolve(stagingDirName);

    Files.createDirectories(stagingDir); // Ensure staging directory exists

    List<Future<Void>> futures = new ArrayList<>();

    try (TarArchiveInputStream tarInput = new TarArchiveInputStream(tarStream)) {
      TarArchiveEntry entry;
      while ((entry = tarInput.getNextTarEntry()) != null) {
        if (!entry.isDirectory()) {
          byte[] fileData = readEntryData(tarInput, entry.getSize());

          // Submit extraction as a task
          TarArchiveEntry finalEntry = entry;
          futures.add(executor.submit(() -> {
            writeFile(stagingDir, finalEntry.getName(), fileData);
            return null;
          }));
        } else {
          File dir = new File(stagingDir.toFile(), entry.getName());
          if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Failed to create directory: " + dir);
          }
        }
      }
    }

    // Wait for all tasks to complete
    for (Future<Void> future : futures) {
      future.get();
    }

    // Move staging to outputDir atomically
    if (Files.exists(outputDir)) {
      FileUtils.deleteDirectory(outputDir.toFile()); // Clean old data
    }
    try {
      Files.move(stagingDir, outputDir, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      LOG.warn("Atomic move of staging dir : {} to {} failed.", stagingDir, outputDir, e);
    }

    LOG.info("Tar extraction completed and moved from staging to: {}", outputDir);
  }

  private byte[] readEntryData(TarArchiveInputStream tarInput, long size) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    byte[] fileData = new byte[(int) size];
    int bytesRead;
    while (size > 0 && (bytesRead = tarInput.read(fileData, 0, (int) Math.min(fileData.length, size))) != -1) {
      buffer.write(fileData, 0, bytesRead);
      size -= bytesRead;
    }
    return buffer.toByteArray();
  }

  private void writeFile(Path outputDir, String fileName, byte[] fileData) {
    try {
      File outputFile = new File(outputDir.toFile(), fileName);
      Path parentDir = outputFile.toPath().getParent();
      if (parentDir != null && !Files.exists(parentDir)) {
        Files.createDirectories(parentDir);
      }

      try (InputStream fis = new ByteArrayInputStream(fileData);
           OutputStream fos = Files.newOutputStream(outputFile.toPath())) {
        byte[] buffer = new byte[8192];  // Use a buffer for efficient reading/writing
        int bytesRead;
        while ((bytesRead = fis.read(buffer)) != -1) {
          fos.write(buffer, 0, bytesRead);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Error writing file: " + fileName, e);
    }
  }

  public void start() {
    if (executorServiceStarted.compareAndSet(false, true)) {
      this.executor =
          new ThreadPoolExecutor(0, threadPoolSize, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), threadFactory);
    }
  }

  public void stop() {
    if (executorServiceStarted.compareAndSet(true, false)) {
      executor.shutdown();
      try {
        if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
          LOG.warn("Tar Extractor Executor Service did not terminate in time.");
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted during shutdown. Forcing shutdown of Tar Extractor Executor Service...");
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }
}


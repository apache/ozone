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

import static org.apache.hadoop.ozone.recon.ReconUtils.createTarFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link TarExtractor}.
 */
public class TestTarExtractor {

  @TempDir
  private Path temporaryFolder;

  @Test
  public void testExtractTarUsesMultipleThreadsForSingleTar() throws Exception {
    Path sourceDir = Files.createDirectory(temporaryFolder.resolve("source"));
    int poolSize = 4;
    int fileCount = poolSize * 4;
    byte[] payload = new byte[4 * 1024 * 1024];
    Arrays.fill(payload, (byte) 'a');
    for (int i = 0; i < fileCount; i++) {
      byte[] content = Arrays.copyOf(payload, payload.length + 1);
      content[payload.length] = (byte) ('0' + (i % 10));
      Files.write(sourceDir.resolve("file" + i + ".sst"), content);
    }

    File tarFile = createTarFile(sourceDir);
    Path outputDir = temporaryFolder.resolve("output");
    String threadPrefix = "TestTarExtractorParallel-";

    TarExtractor extractor = new TarExtractor(poolSize, threadPrefix);
    Set<String> writeFileThreads = ConcurrentHashMap.newKeySet();
    AtomicReference<Throwable> extractionError = new AtomicReference<>();

    extractor.start();
    Thread extractionThread = new Thread(() -> {
      try (InputStream input = Files.newInputStream(tarFile.toPath())) {
        extractor.extractTar(input, outputDir);
      } catch (Throwable t) {
        extractionError.set(t);
      }
    });

    try {
      extractionThread.start();

      long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (extractionThread.isAlive()
          && System.nanoTime() < deadlineNanos
          && writeFileThreads.size() < poolSize) {
        Thread.getAllStackTraces().forEach((thread, stackTrace) -> {
          if (thread.getName().startsWith(threadPrefix)
              && isInTarExtractorWritePath(stackTrace)) {
            writeFileThreads.add(thread.getName());
          }
        });
        Thread.sleep(1);
      }

      extractionThread.join(TimeUnit.SECONDS.toMillis(30));
      assertFalse(extractionThread.isAlive(), "Tar extraction did not finish in time");

      if (extractionError.get() != null) {
        throw new AssertionError("Tar extraction failed", extractionError.get());
      }

      assertEquals(poolSize, writeFileThreads.size(),
          "Expected writeFile to run in parallel across all configured worker threads");
    } finally {
      extractor.stop();
    }
  }

  private static boolean isInTarExtractorWritePath(StackTraceElement[] stackTrace) {
    for (StackTraceElement stackElement : stackTrace) {
      if (TarExtractor.class.getName().equals(stackElement.getClassName())
          && "writeFile".equals(stackElement.getMethodName())) {
        return true;
      }
    }
    return false;
  }
}

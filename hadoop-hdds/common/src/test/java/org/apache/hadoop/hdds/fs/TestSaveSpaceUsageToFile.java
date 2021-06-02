/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.fs;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.OptionalLong;

import static org.apache.ozone.test.GenericTestUtils.getTestDir;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link SaveSpaceUsageToFile}.
 */
public class TestSaveSpaceUsageToFile {

  private static final File DIR =
      getTestDir(TestSaveSpaceUsageToFile.class.getSimpleName());

  private static final Duration LONG_EXPIRY = Duration.ofMinutes(15);

  private static final SpaceUsageSource VALID_USAGE_SOURCE =
      MockSpaceUsageSource.fixed(123, 81);

  private File file;

  @Before
  public void setup() {
    FileUtil.fullyDelete(DIR);
    assertTrue(DIR.mkdirs());
    file = new File(DIR, "space_usage.txt");
  }

  @After
  public void cleanup() {
    FileUtil.fullyDelete(DIR);
  }

  @Test
  public void persistsValidValue() {
    SpaceUsagePersistence subject = new SaveSpaceUsageToFile(file, LONG_EXPIRY);

    subject.save(VALID_USAGE_SOURCE);
    OptionalLong savedValue = subject.load();

    assertTrue(file.exists());
    assertTrue(savedValue.isPresent());
    assertEquals(VALID_USAGE_SOURCE.getUsedSpace(), savedValue.getAsLong());
  }

  @Test
  public void doesNotSaveInvalidValue() {
    SpaceUsageSource source = MockSpaceUsageSource.fixed(123, 123);
    SpaceUsagePersistence subject = new SaveSpaceUsageToFile(file, LONG_EXPIRY);

    subject.save(source);
    OptionalLong savedValue = subject.load();

    assertFalse(file.exists());
    assertFalse(savedValue.isPresent());
  }

  @Test
  public void doesNotLoadExpiredValue() throws Exception {
    Duration shortExpiry = Duration.ofMillis(5);
    SpaceUsagePersistence subject = new SaveSpaceUsageToFile(file, shortExpiry);

    subject.save(VALID_USAGE_SOURCE);
    Instant expired = Instant.now().plus(shortExpiry);
    waitFor(() -> Instant.now().isAfter(expired), 10, 1000);
    OptionalLong savedValue = subject.load();

    assertTrue(file.exists());
    assertFalse(savedValue.isPresent());
  }

  @Test
  public void doesNotLoadIfTimeMissing() throws IOException {
    saveToFile(Long.toString(VALID_USAGE_SOURCE.getUsedSpace()));
    SpaceUsagePersistence subject = new SaveSpaceUsageToFile(file, LONG_EXPIRY);

    OptionalLong savedValue = subject.load();

    assertFalse(savedValue.isPresent());
  }

  @Test
  public void doesNotLoadIfFileMissing() {
    SpaceUsagePersistence subject = new SaveSpaceUsageToFile(file, LONG_EXPIRY);

    OptionalLong savedValue = subject.load();

    assertFalse(file.exists());
    assertFalse(savedValue.isPresent());
  }

  @Test
  public void doesNotLoadGarbage() throws IOException {
    saveToFile("garbage 456");
    SpaceUsagePersistence subject = new SaveSpaceUsageToFile(file, LONG_EXPIRY);

    OptionalLong savedValue = subject.load();

    assertFalse(savedValue.isPresent());
  }

  @Test
  public void overwritesExistingFile() throws IOException {
    saveToFile("456 " + Instant.now().toEpochMilli());
    SpaceUsagePersistence subject = new SaveSpaceUsageToFile(file, LONG_EXPIRY);

    subject.save(VALID_USAGE_SOURCE);
    OptionalLong savedValue = subject.load();

    assertTrue(savedValue.isPresent());
    assertEquals(VALID_USAGE_SOURCE.getUsedSpace(), savedValue.getAsLong());
  }

  private void saveToFile(String content) throws IOException {
    FileUtils.writeStringToFile(file, content, StandardCharsets.UTF_8);
  }

}

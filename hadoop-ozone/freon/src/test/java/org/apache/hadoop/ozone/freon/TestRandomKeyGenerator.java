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

package org.apache.hadoop.ozone.freon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Unit tests for RandomKeyGenerator command.
 */
public class TestRandomKeyGenerator {

  @Test
  void rejectsNegativeNumOfVolumes() {
    assertValidationFails(
        "--num-of-volumes must be a positive integer",
        "--num-of-volumes", "-1",
        "--num-of-buckets", "1",
        "--num-of-keys", "1");
  }

  @Test
  void rejectsZeroNumOfBuckets() {
    assertValidationFails(
        "--num-of-buckets must be a positive integer",
        "--num-of-volumes", "1",
        "--num-of-buckets", "0",
        "--num-of-keys", "1");
  }

  @Test
  void rejectsNegativeNumOfKeys() {
    assertValidationFails(
        "--num-of-keys must be a positive integer",
        "--num-of-volumes", "1",
        "--num-of-buckets", "1",
        "--num-of-keys", "-1");
  }

  @Test
  void rejectsNegativeNumOfThreads() {
    assertValidationFails(
        "--num-of-threads must be a positive integer",
        "--num-of-volumes", "1",
        "--num-of-buckets", "1",
        "--num-of-keys", "1",
        "--num-of-threads", "-1");
  }

  @Test
  void rejectsNegativeNumOfValidateThreadsWhenValidateWritesEnabled() {
    assertValidationFails(
        "--num-of-validate-threads must be a positive integer",
        "--num-of-volumes", "1",
        "--num-of-buckets", "1",
        "--num-of-keys", "1",
        "--validate-writes",
        "--num-of-validate-threads", "-1");
  }

  private void assertValidationFails(String expectedMessage, String... args) {
    RandomKeyGenerator generator = new RandomKeyGenerator(new OzoneConfiguration());
    CommandLine cmd = new CommandLine(generator);
    cmd.parseArgs(args);

    IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class, generator::call);

    assertThat(ex.getMessage()).contains(expectedMessage);
  }
}

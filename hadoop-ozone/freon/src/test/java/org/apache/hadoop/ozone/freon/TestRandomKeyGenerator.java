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
        new String[] {"--num-of-volumes", "-1",
            "--num-of-buckets", "1",
            "--num-of-keys", "1"},
        "numOfVolumes must be a positive integer");
  }

  @Test
  void rejectsZeroNumOfBuckets() {
    assertValidationFails(
        new String[] {"--num-of-volumes", "1",
            "--num-of-buckets", "0",
            "--num-of-keys", "1"},
        "numOfBuckets must be a positive integer");
  }

  @Test
  void rejectsNegativeNumOfKeys() {
    assertValidationFails(
        new String[] {"--num-of-volumes", "1",
            "--num-of-buckets", "1",
            "--num-of-keys", "-1"},
        "numOfKeys must be a positive integer");
  }

  private void assertValidationFails(String[] args, String expectedMessage) {
    RandomKeyGenerator generator = new RandomKeyGenerator(new OzoneConfiguration());
    CommandLine cmd = new CommandLine(generator);
    cmd.parseArgs(args);

    IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class, generator::call);

    assertThat(ex.getMessage()).contains(expectedMessage);
  }
}

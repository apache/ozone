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

package org.apache.hadoop.ozone.debug.replicas;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Unit tests for replicas verify command option parsing.
 */
public class TestReplicasVerify {

  @Test
  void testRefreshContainerLocationsFromScmOption() throws Exception {
    ReplicasVerify command = new ReplicasVerify();

    new CommandLine(command).parseArgs("--checksums", "--refresh-from-scm", "/volume1");

    assertThat(isRefreshContainerLocationsFromScmEnabled(command)).isTrue();
  }

  @Test
  void testRefreshContainerLocationsFromScmDefault() throws Exception {
    ReplicasVerify command = new ReplicasVerify();

    new CommandLine(command).parseArgs("--checksums", "/volume1");

    assertThat(isRefreshContainerLocationsFromScmEnabled(command)).isFalse();
  }

  private boolean isRefreshContainerLocationsFromScmEnabled(ReplicasVerify command) throws Exception {
    Field field = ReplicasVerify.class.getDeclaredField("refreshContainerLocationsFromScm");
    field.setAccessible(true);
    return field.getBoolean(command);
  }
}

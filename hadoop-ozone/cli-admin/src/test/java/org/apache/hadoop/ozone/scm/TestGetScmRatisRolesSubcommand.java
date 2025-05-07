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

package org.apache.hadoop.ozone.scm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.ozone.admin.scm.GetScmRatisRolesSubcommand;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * This unit test is used to verify whether the output of
 * `TestGetScmRatisRolesSubcommand` meets the expected results.
 */
public class TestGetScmRatisRolesSubcommand {

  @Test
  public void testGetScmHARatisRoles() throws Exception {
    GetScmRatisRolesSubcommand cmd = new GetScmRatisRolesSubcommand();
    ScmClient client = mock(ScmClient.class);
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--table");

    List<String> result = new ArrayList<>();
    result.add("bigdata-ozone-online31:9894:FOLLOWER:61b1c8e5-da40-4567-8a17-96a0234ba14e:100.3.197.98");
    result.add("bigdata-ozone-online32:9894:LEADER:e428ca07-b2a3-4756-bf9b-a4abb033c7d1:100.3.192.89");
    result.add("bigdata-ozone-online30:9894:FOLLOWER:41f90734-b3ee-4284-ad96-40a286654952:100.3.196.51");

    when(client.getScmRoles()).thenAnswer(invocation -> result);

    try (GenericTestUtils.SystemOutCapturer capture =
        new GenericTestUtils.SystemOutCapturer()) {
      cmd.execute(client);
      assertThat(capture.getOutput()).contains(
          "bigdata-ozone-online31 |    9894    | FOLLOWER | 61b1c8e5-da40-4567-8a17-96a0234ba14e");
      assertThat(capture.getOutput()).contains(
          "bigdata-ozone-online32 |    9894    |  LEADER  | e428ca07-b2a3-4756-bf9b-a4abb033c7d1");
      assertThat(capture.getOutput()).contains(
          "bigdata-ozone-online30 |    9894    | FOLLOWER | 41f90734-b3ee-4284-ad96-40a286654952");
    }
  }

}

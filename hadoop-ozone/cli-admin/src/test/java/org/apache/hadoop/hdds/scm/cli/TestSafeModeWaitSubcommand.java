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

package org.apache.hadoop.hdds.scm.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.security.AccessControlException;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Tests for `ozone admin safemode wait`.
 */
public class TestSafeModeWaitSubcommand {

  private static SafeModeWaitSubcommand newCommand(String timeoutSeconds) {
    SafeModeWaitSubcommand cmd = new SafeModeWaitSubcommand();
    new CommandLine(cmd).parseArgs("-t", timeoutSeconds);
    return cmd;
  }

  @Test
  void retriesUntilOutOfSafeMode() throws Exception {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.inSafeMode()).thenThrow(new IOException("SCM is not available")).thenReturn(true, false);

    assertTimeoutPreemptively(Duration.ofSeconds(10), () -> newCommand("30").execute(scmClient));
    verify(scmClient, times(3)).inSafeMode();
  }

  @Test
  void timesOutInSafeMode() throws Exception {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.inSafeMode()).thenThrow(new IOException("SCM is not available")).thenReturn(true);

    TimeoutException e = assertThrows(TimeoutException.class, () -> newCommand("2").execute(scmClient));
    // The last error is kept as the cause.
    assertThat(e).hasRootCauseMessage("SCM is not available");
  }

  @Test
  void failsOnAccessControlException() throws Exception {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.inSafeMode()).thenThrow(new AccessControlException("denied"));

    assertThrows(AccessControlException.class, () -> newCommand("30").execute(scmClient));
    verify(scmClient, times(1)).inSafeMode();
  }

  @Test
  void timesOutWhileSCMCallIsStillRetrying() throws Exception {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.inSafeMode()).thenAnswer(invocation -> {
      Thread.sleep(Long.MAX_VALUE);
      return false;
    });

    assertTimeoutPreemptively(Duration.ofSeconds(10),
        () -> assertThrows(TimeoutException.class, () -> newCommand("1").execute(scmClient)));
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.repair;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.fs.ozone.Constants.OZONE_DEFAULT_USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * Tests the ozone repair command.
 */
public class TestOzoneRepair {

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;
  private static final String DEFAULT_ENCODING = UTF_8.name();

  @BeforeEach
  public void setup() throws Exception {
    System.setOut(new PrintStream(out, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(err, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void reset() {
    // reset stream after each unit test
    out.reset();
    err.reset();

    // restore system streams
    System.setOut(OLD_OUT);
    System.setErr(OLD_ERR);
  }

  @Test
  void testOzoneRepairWhenUserIsOzoneDefaultUser() throws Exception {
    OzoneRepair ozoneRepair = spy(new OzoneRepair());
    doReturn(OZONE_DEFAULT_USER).when(ozoneRepair).getSystemUserName();

    ozoneRepair.execute(new String[]{});
    assertThat(out.toString(DEFAULT_ENCODING)).contains("Run as user: " + OZONE_DEFAULT_USER);
  }

  @Test
  void testOzoneRepairWhenUserIsNotOzoneDefaultUserAndDeclinesToProceed() throws Exception {
    OzoneRepair ozoneRepair = spy(new OzoneRepair());
    doReturn("non-ozone").when(ozoneRepair).getSystemUserName();
    doReturn("N").when(ozoneRepair).getConsoleReadLineWithFormat(anyString(), anyString());

    int res = ozoneRepair.execute(new String[]{});
    assertEquals(1, res);
    assertThat(out.toString(DEFAULT_ENCODING)).contains("Aborting command.");
  }

  @Test
  void testOzoneRepairWhenUserIsNotOzoneDefaultUserAndAgreesToProceed() throws Exception {
    OzoneRepair ozoneRepair = spy(new OzoneRepair());
    String currentUser = "non-ozone";
    doReturn(currentUser).when(ozoneRepair).getSystemUserName();
    doReturn("y").when(ozoneRepair).getConsoleReadLineWithFormat(anyString(), anyString());

    ozoneRepair.execute(new String[]{});
    assertThat(out.toString(DEFAULT_ENCODING)).contains("Run as user: " + currentUser);
  }

}

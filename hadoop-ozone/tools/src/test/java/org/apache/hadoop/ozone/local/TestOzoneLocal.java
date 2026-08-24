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

package org.apache.hadoop.ozone.local;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Tests for {@link OzoneLocal}.
 */
class TestOzoneLocal {

  @Test
  void localCommandMetadataIsPresentAndHidden() {
    Command command = OzoneLocal.class.getAnnotation(Command.class);

    assertNotNull(command);
    assertEquals("ozone local", command.name());
    assertTrue(command.hidden());
  }

  @Test
  void runCommandMetadataIsPresentAndHidden() {
    Command command = OzoneLocal.RunCommand.class.getAnnotation(Command.class);

    assertNotNull(command);
    assertEquals("run", command.name());
    assertTrue(command.hidden());
  }

  @Test
  void genericCliRegistersRunPlaceholder() {
    OzoneLocal local = new OzoneLocal();

    assertTrue(local.getCmd().getSubcommands().containsKey("run"));
  }

  @Test
  void rootHelpHidesRunPlaceholder() throws Exception {
    OzoneLocal local = new OzoneLocal();
    CommandLine commandLine = local.getCmd();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    commandLine.setOut(new PrintWriter(new OutputStreamWriter(out, UTF_8),
        true));
    commandLine.setErr(new PrintWriter(new OutputStreamWriter(err, UTF_8),
        true));

    int exitCode = local.execute(new String[] {"--help"});

    String help = out.toString(UTF_8.name());
    assertEquals(0, exitCode);
    assertTrue(help.contains("Usage: ozone local"));
    assertFalse(help.contains("run"));
    assertEquals("", err.toString(UTF_8.name()));
  }

  @Test
  void runCommandIsQuietNoOpPlaceholder() throws Exception {
    OzoneLocal local = new OzoneLocal();
    CommandLine commandLine = local.getCmd();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    commandLine.setOut(new PrintWriter(new OutputStreamWriter(out, UTF_8),
        true));
    commandLine.setErr(new PrintWriter(new OutputStreamWriter(err, UTF_8),
        true));

    int exitCode = local.execute(new String[] {"run"});

    assertEquals(0, exitCode);
    assertEquals("", out.toString(UTF_8.name()));
    assertEquals("", err.toString(UTF_8.name()));
  }
}

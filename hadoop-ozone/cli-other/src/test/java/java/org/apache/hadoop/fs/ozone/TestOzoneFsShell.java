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

package org.apache.hadoop.fs.ozone;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.hdds.tracing.TestSpanCollector;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.Test;

/**
 * Tests the behavior of OzoneFsShell.
 */
public class TestOzoneFsShell {

  // tests command handler for FsShell bound to OzoneDelete class
  @Test
  public void testOzoneFsShellRegisterDeleteCmd() throws Exception {
    final String rmCmdName = "rm";
    final String rmCmd = "-" + rmCmdName;
    final String arg = "arg1";
    OzoneFsShell shell = new OzoneFsShell();
    String[] argv = {arg, arg};
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    PrintStream bytesPrintStream = new PrintStream(bytes, false, UTF_8.name());
    PrintStream oldErr = System.err;
    System.setErr(bytesPrintStream);
    try {
      ToolRunner.run(shell, argv);

      // test command bindings for "rm" command handled by OzoneDelete class
      CommandFactory factory = shell.getCommandFactory();
      assertNotNull(factory);
      assertEquals(1, Arrays.stream(factory.getNames())
          .filter(c -> c.equals(rmCmd)).count());
      Command instance = factory.getInstance(rmCmd);
      assertNotNull(instance);
      assertEquals(OzoneFsDelete.Rm.class, instance.getClass());
      assertEquals(rmCmdName, instance.getCommandName());
    } finally {
      shell.close();
      System.setErr(oldErr);
    }
  }

  @Test
  public void testParseCommandNameIsCommandWithoutArguments() {
    assertEquals("ozone fs -ls", OzoneFsShell.parseCommandName(new String[] {"-ls", "-R", "/vol1/bucket1"}));
    assertEquals("ozone fs -put",
        OzoneFsShell.parseCommandName(new String[] {"-D", "ozone.om.address=om1", "-put", "/tmp/a", "/vol1/b/a"}));
    assertEquals("ozone fs", OzoneFsShell.parseCommandName(new String[0]));
  }

  @Test
  public void testCommandAttributesHaveOptionsAndArgs() {
    String[] argv = {"-fs", "ofs://om1", "-put", "-f", "/tmp/a", "/vol1/bucket1/a"};

    try (TestSpanCollector collector = TestSpanCollector.install("FsShell")) {
      TracingUtil.executeInNewSpan("request", () -> OzoneFsShell.addCommandAttributes(argv));

      collector.assertAttribute("request", FsShellTracingAttributes.CLI_COMMAND, "ozone fs -put");
      collector.assertAttribute("request", FsShellTracingAttributes.CLI_OPTION_PREFIX + "-fs", "ofs://om1");
      collector.assertAttribute("request", FsShellTracingAttributes.CLI_OPTION_PREFIX + "-f", true);
      collector.assertAttribute("request", FsShellTracingAttributes.CLI_ARGS, "/tmp/a /vol1/bucket1/a");
    }
  }

  @Test
  public void testRedactedOptionsAreNotAdded() {
    String[] argv = {"-D", "fs.s3a.secret.key=mySecret", "-Dozone.om.address=om1", "-tokenCacheFile", "/tmp/tokens",
        "-ls", "/vol1"};

    try (TestSpanCollector collector = TestSpanCollector.install("FsShell")) {
      TracingUtil.executeInNewSpan("request", () -> OzoneFsShell.addCommandAttributes(argv));

      collector.assertAttribute("request", FsShellTracingAttributes.CLI_COMMAND, "ozone fs -ls");
      collector.assertNoAttribute("request", FsShellTracingAttributes.CLI_OPTION_PREFIX + "-D");
      collector.assertNoAttribute("request", FsShellTracingAttributes.CLI_OPTION_PREFIX + "-Dozone.om.address=om1");
      collector.assertNoAttribute("request", FsShellTracingAttributes.CLI_OPTION_PREFIX + "-tokenCacheFile");
      collector.assertAttribute("request", FsShellTracingAttributes.CLI_ARGS, "/vol1");
    }
  }
}

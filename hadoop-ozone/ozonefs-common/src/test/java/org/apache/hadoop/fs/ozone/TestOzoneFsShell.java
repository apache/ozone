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
package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.util.ToolRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.junit.Assert;
import org.junit.Test;


/**
 * Tests the behavior of OzoneFsShell.
 */
public class TestOzoneFsShell {

  // tests command handler for FsShell bound to OzoneDelete class
  @Test
  public void testOzoneFsShellRegisterDeleteCmd() throws IOException {
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
    } catch (Exception e) {
    } finally {
      // test command bindings for "rm" command handled by OzoneDelete class
      CommandFactory factory = shell.getCommandFactory();
      Assert.assertEquals(1, Arrays.stream(factory.getNames())
          .filter(c -> c.equals(rmCmd)).count());
      Command instance = factory.getInstance(rmCmd);
      Assert.assertNotNull(instance);
      Assert.assertEquals(OzoneFsDelete.Rm.class, instance.getClass());
      Assert.assertEquals(rmCmdName, instance.getCommandName());
      shell.close();
      System.setErr(oldErr);
    }
  }
}

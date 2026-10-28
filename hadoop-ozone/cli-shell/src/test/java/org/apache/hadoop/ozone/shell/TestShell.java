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

package org.apache.hadoop.ozone.shell;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Tests the root span name and attributes {@link Shell} builds from the parsed command line.
 */
class TestShell {

  @Test
  void testParseCommandNameIsPathWithoutArguments() {
    CommandLine.ParseResult parseResult = parse("key", "put", "vol1/bucket1/key1", "/tmp/file", "--type", "RATIS");

    assertEquals("ozone sh key put", Shell.parseCommandName(parseResult));
  }

  @Test
  void testParseCommandAttributesHasOptionsAndArgs() {
    CommandLine.ParseResult parseResult = parse("--verbose", "key", "put", "vol1/bucket1/key1", "/tmp/file",
        "--type", "RATIS");

    Map<String, String> expected = new LinkedHashMap<>();
    expected.put(ShellTracingAttributes.CLI_COMMAND, "ozone sh key put");
    expected.put(ShellTracingAttributes.CLI_OPTION_PREFIX + "--verbose", "true");
    expected.put(ShellTracingAttributes.CLI_OPTION_PREFIX + "--type", "RATIS");
    expected.put(ShellTracingAttributes.CLI_ARGS, "vol1/bucket1/key1 /tmp/file");
    assertEquals(expected, Shell.parseCommandAttributes(parseResult));
  }

  @Test
  void testSensitiveOptionValuesAreRedacted() {
    CommandLine.ParseResult parseResult = parse("-D", "ozone.om.address=om1", "s3", "setsecret", "-u", "alice",
        "-s", "mySecretValue");

    Map<String, String> attributes = Shell.parseCommandAttributes(parseResult);

    assertEquals("ozone sh s3 setsecret", attributes.get(ShellTracingAttributes.CLI_COMMAND));
    assertEquals("alice", attributes.get(ShellTracingAttributes.CLI_OPTION_PREFIX + "-u"));
    assertEquals(Shell.REDACTED, attributes.get(ShellTracingAttributes.CLI_OPTION_PREFIX + "--secret"));
    assertEquals(Shell.REDACTED, attributes.get(ShellTracingAttributes.CLI_OPTION_PREFIX + "--set"));
  }

  private static CommandLine.ParseResult parse(String... args) {
    return new CommandLine(new Root()).parseArgs(args);
  }

  @Command(name = "ozone sh", subcommands = {Key.class, S3.class})
  static class Root {
    @Option(names = "--verbose")
    private boolean verbose;

    @Option(names = {"-D", "--set"})
    private Map<String, String> config;
  }

  @Command(name = "key", subcommands = Put.class)
  static class Key {
  }

  @Command(name = "put")
  static class Put {
    @Parameters
    private List<String> args;

    @Option(names = {"-t", "--type"})
    private String type;
  }

  @Command(name = "s3", subcommands = SetSecret.class)
  static class S3 {
  }

  @Command(name = "setsecret")
  static class SetSecret {
    @Option(names = "-u")
    private String user;

    @Option(names = {"-s", "--secret"})
    private String secret;
  }
}

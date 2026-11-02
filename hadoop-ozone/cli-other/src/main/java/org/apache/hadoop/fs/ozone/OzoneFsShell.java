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

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.trace.SpanKind;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.fs.shell.FsCommand;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.util.ToolRunner;

/** Provide command line access to a Ozone FileSystem. */
@InterfaceAudience.Private
public class OzoneFsShell extends FsShell {

  private static final String OZONE_USAGE_PREFIX = "Usage: ozone fs [generic options]";

  static final Set<String> GENERIC_OPTIONS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "-conf", "-D", "-fs", "-jt", "-files", "-libjars", "-archives", "-tokenCacheFile")));

  static final Set<String> REDACTED_OPTIONS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "-D", "-tokenCacheFile")));

  /**
   * Default ctor with no configuration.  Be sure to invoke
   * {@link #setConf(Configuration)} with a valid configuration prior
   * to running commands.
   */
  public OzoneFsShell() {
    this(null);
  }

  /**
   * Construct a OzoneFsShell with the given configuration.
   *
   * Commands can be executed via {@link #run(String[])}
   * @param conf the hadoop configuration
   */
  public OzoneFsShell(OzoneConfiguration conf) {
    super(conf);
  }

  @Override
  protected void registerCommands(CommandFactory factory) {
    // TODO: DFSAdmin subclasses FsShell so need to protect the command
    // registration.  This class should morph into a base class for
    // commands, and then this method can be abstract
    if (this.getClass().equals(OzoneFsShell.class)) {
      factory.registerCommands(FsCommand.class);
      // ozone delete rm command registration supersedes fs delete
      factory.registerCommands(OzoneFsDelete.class);
    }
  }

  @Override
  protected String getUsagePrefix() {
    return OZONE_USAGE_PREFIX;
  }

  /**
   * Main entry point to execute fs commands.
   *
   * @param argv the command and its arguments
   * @throws Exception upon error
   */
  public static void main(String[] argv) throws Exception {
    OzoneFsShell shell = newShellInstance();
    OzoneConfiguration conf = new OzoneConfiguration();
    int res = TracingUtil.execute("FsShell", parseCommandName(argv), SpanKind.CLIENT, conf, () -> {
      addCommandAttributes(argv);
      conf.setQuietMode(false);
      shell.setConf(conf);
      return shell.execute(argv);
    });

    System.exit(res);
  }

  private int execute(String[] argv) throws Exception {
    try {
      return ToolRunner.run(this, argv);
    } finally {
      close();
    }
  }

  // TODO: this should be abstract in a base class
  protected static OzoneFsShell newShellInstance() {
    return new OzoneFsShell();
  }

  // for testing purposes, ensure that ozone specific
  // added fs commands are visible
  @VisibleForTesting
  public CommandFactory getCommandFactory() {
    return commandFactory;
  }

  /** @return the FsShell command that was run, for example {@code ozone fs -ls}, without options or arguments. */
  static String parseCommandName(String[] argv) {
    for (int i = 0; i < argv.length; i++) {
      if (GENERIC_OPTIONS.contains(argv[i])) {
        i++;
      } else if (!argv[i].startsWith("-D")) {
        return "ozone fs " + argv[i];
      }
    }
    return "ozone fs";
  }

  /** Adds the command, its options and its arguments to the current span; {@link #REDACTED_OPTIONS} are skipped. */
  static void addCommandAttributes(String[] argv) {
    TracingUtil.addAttributesToCurrentSpan(FsShellTracingAttributes.CLI_COMMAND, parseCommandName(argv));
    List<String> args = new ArrayList<>();
    for (int i = 0; i < argv.length; i++) {
      String arg = argv[i];
      if (REDACTED_OPTIONS.contains(arg)) {
        i++;
      } else if (GENERIC_OPTIONS.contains(arg) && i + 1 < argv.length) {
        TracingUtil.addAttributesToCurrentSpan(FsShellTracingAttributes.CLI_OPTION_PREFIX + arg, argv[++i]);
      } else if (!arg.startsWith("-")) {
        args.add(arg);
      } else if (!arg.startsWith("-D")) {
        TracingUtil.addAttributesToCurrentSpan(FsShellTracingAttributes.CLI_OPTION_PREFIX + arg, true);
      }
    }
    if (!args.isEmpty()) {
      TracingUtil.addAttributesToCurrentSpan(FsShellTracingAttributes.CLI_ARGS, String.join(" ", args));
    }
  }
}

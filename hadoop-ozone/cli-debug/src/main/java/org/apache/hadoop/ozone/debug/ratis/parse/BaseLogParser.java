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

package org.apache.hadoop.ozone.debug.ratis.parse;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.util.function.Function;
import org.apache.hadoop.hdds.cli.DeprecatedCliOptions;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.tools.ParseRatisLog;
import picocli.CommandLine;

/**
 * Base Ratis Log Parser used by generic, datanode etc.
 */
public abstract class BaseLogParser {
  @CommandLine.Option(names = {"-s", "--segment-path"},
      description = "Path of the segment file")
  private File segmentFile;

  /** For backward compatibility. */
  @Deprecated
  @SuppressWarnings("DeprecatedIsStillUsed")
  @CommandLine.Option(names = "--segmentPath", hidden = true)
  private File deprecatedSegmentFile;

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  private File resolveSegmentFile() {
    DeprecatedCliOptions.warnIfDeprecatedUsedWithoutCanonical(
        "--segmentPath", "--segment-path", spec, "--segment-path", "-s");
    File resolved;
    if (DeprecatedCliOptions.hasMatchedOption(spec, "--segmentPath")) {
      resolved = deprecatedSegmentFile;
    } else {
      resolved = segmentFile;
    }
    if (resolved == null) {
      throw new CommandLine.ParameterException(spec.commandLine(),
          "Missing required option '--segment-path=<segmentFile>'");
    }
    return resolved;
  }

  public void parseRatisLogs(
      Function<RaftProtos.StateMachineLogEntryProto, String> smLogToStr) {
    try {
      ParseRatisLog.Builder builder = new ParseRatisLog.Builder();
      builder.setSegmentFile(resolveSegmentFile());
      builder.setSMLogToString(smLogToStr);

      ParseRatisLog prl = builder.build();
      prl.dumpSegmentFile();
    } catch (Exception e) {
      System.out.println(RatisLogParser.class.getSimpleName()
          + "failed with exception  " + e);
    }
  }

  @VisibleForTesting
  public void setSegmentFile(File segmentFile) {
    this.segmentFile = segmentFile;
  }
}

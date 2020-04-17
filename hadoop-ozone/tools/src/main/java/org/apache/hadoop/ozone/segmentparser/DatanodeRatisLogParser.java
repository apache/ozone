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
package org.apache.hadoop.ozone.segmentparser;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.container.common.transport.server
    .ratis.ContainerStateMachine;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * Command line utility to parse and dump a datanode ratis segment file.
 */
@CommandLine.Command(
    name = "datanode",
    description = "dump datanode segment file",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DatanodeRatisLogParser extends BaseLogParser
    implements Callable<Void> {
  @CommandLine.ParentCommand
  private RatisLogParser logParser;

  private static final RaftGroupId DUMMY_PIPELINE_ID =
      RaftGroupId.valueOf(ByteString.copyFromUtf8("ADummyRatisGroup"));

  public static String smToContainerLogString(
      StateMachineLogEntryProto logEntryProto) {
    return ContainerStateMachine.
        smProtoToString(DUMMY_PIPELINE_ID, null, logEntryProto);
  }

  @Override
  public Void call() throws Exception {
    System.out.println("Dumping Datanode Ratis Log");
    System.out.println("Using Dummy PipelineID:" + DUMMY_PIPELINE_ID + "\n\n");

    parseRatisLogs(DatanodeRatisLogParser::smToContainerLogString);
    return null;
  }
}

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
package org.apache.hadoop.hdds.scm.cli.container;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Visibility;
import picocli.CommandLine.Option;

/**
 * This is the handler that process container list command.
 */
@Command(
    name = "list",
    description = "List containers",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ListSubcommand extends ScmSubcommand {

  private static final Logger LOG =
      LoggerFactory.getLogger(ListSubcommand.class);

  @Option(names = {"-s", "--start"},
      description = "Container id to start the iteration")
  private long startId;

  @Option(names = {"-c", "--count"},
      description = "Maximum number of containers to list",
      defaultValue = "20", showDefaultValue = Visibility.ALWAYS)
  private int count;

  @Option(names = {"--state"},
      description = "Container state(OPEN, CLOSING, QUASI_CLOSED, CLOSED, " +
          "DELETING, DELETED)")
  private HddsProtos.LifeCycleState state;

  private static final ObjectWriter WRITER;

  static {
    ObjectMapper mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    mapper
        .setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE);
    WRITER = mapper.writerWithDefaultPrettyPrinter();
  }


  private void outputContainerInfo(ContainerInfo containerInfo)
      throws IOException {
    // Print container report info.
    LOG.info("{}", WRITER.writeValueAsString(containerInfo));
  }

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    List<ContainerInfo> containerList =
        scmClient.listContainer(startId, count, state);

    // Output data list
    for (ContainerInfo container : containerList) {
      outputContainerInfo(container);
    }
  }
}

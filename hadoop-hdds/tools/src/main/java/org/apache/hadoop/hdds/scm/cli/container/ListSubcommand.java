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

import com.google.common.base.Strings;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.container.ContainerListResult;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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

  @Option(names = {"-s", "--start"},
      description = "Container id to start the iteration")
  private long startId;

  @Option(names = {"-c", "--count"},
      description = "Maximum number of containers to list.",
      defaultValue = "20", showDefaultValue = Visibility.ALWAYS)
  private int count;

  @Option(names = {"-a", "--all"},
      description = "List all containers.",
      defaultValue = "false")
  private boolean all;

  @Option(names = {"--state"},
      description = "Container state(OPEN, CLOSING, QUASI_CLOSED, CLOSED, " +
          "DELETING, DELETED)")
  private HddsProtos.LifeCycleState state;

  @Option(names = {"-t", "--type"},
      description = "Replication Type (RATIS, STAND_ALONE or EC)")
  private HddsProtos.ReplicationType type;

  @Option(names = {"-r", "--replication", "--factor"},
      description = "Container replication (ONE, THREE for Ratis, " +
          "rs-6-3-1024k for EC)")
  private String replication;

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
    System.out.println(WRITER.writeValueAsString(containerInfo));
  }

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    if (!Strings.isNullOrEmpty(replication) && type == null) {
      // Set type to RATIS as that is what any command prior to this change
      // would have expected.
      type = HddsProtos.ReplicationType.RATIS;
    }
    ReplicationConfig repConfig = null;
    if (!Strings.isNullOrEmpty(replication)) {
      repConfig = ReplicationConfig.parse(
          ReplicationType.fromProto(type),
          replication, new OzoneConfiguration());
    }

    int maxCountAllowed = getOzoneConf()
        .getInt(ScmConfigKeys.OZONE_SCM_CONTAINER_LIST_MAX_COUNT,
            ScmConfigKeys.OZONE_SCM_CONTAINER_LIST_MAX_COUNT_DEFAULT);

    ContainerListResult containerListAndTotalCount;

    if (!all) {
      if (count > maxCountAllowed) {
        System.err.printf("Attempting to list the first %d records of containers." +
            " However it exceeds the cluster's current limit of %d. The results will be capped at the" +
            " maximum allowed count.%n", count, ScmConfigKeys.OZONE_SCM_CONTAINER_LIST_MAX_COUNT_DEFAULT);
        count = maxCountAllowed;
      }
      containerListAndTotalCount = scmClient.listContainer(startId, count, state, type, repConfig);
      for (ContainerInfo container : containerListAndTotalCount.getContainerInfoList()) {
        outputContainerInfo(container);
      }

      if (containerListAndTotalCount.getTotalCount() > count) {
        System.err.printf("Displaying %d out of %d containers. " +
                        "Container list has more containers.%n",
                count, containerListAndTotalCount.getTotalCount());
      }
    } else {
      // Batch size is either count passed through cli or maxCountAllowed
      int batchSize = (count > 0) ? count : maxCountAllowed;
      long currentStartId = startId;
      int fetchedCount;

      do {
        // Fetch containers in batches of 'batchSize'
        containerListAndTotalCount = scmClient.listContainer(currentStartId, batchSize, state, type, repConfig);
        fetchedCount = containerListAndTotalCount.getContainerInfoList().size();

        for (ContainerInfo container : containerListAndTotalCount.getContainerInfoList()) {
          outputContainerInfo(container);
        }

        if (fetchedCount > 0) {
          currentStartId = containerListAndTotalCount.getContainerInfoList().get(fetchedCount - 1).getContainerID() + 1;
        }
      } while (fetchedCount > 0);
    }
  }
}

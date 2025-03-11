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

package org.apache.hadoop.hdds.scm.cli.container;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Strings;
import java.io.IOException;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerListResult;
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
  
  @Option(names = {"--json"},
      description = "Output the entire list in JSON array format",
      defaultValue = "false")
  private boolean jsonFormat;

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


  private void outputContainerInfo(ContainerInfo containerInfo) throws IOException {
    // Original behavior - just print the container JSON
    System.out.println(WRITER.writeValueAsString(containerInfo));
  }
  
  private void outputContainerInfoAsJsonMember(ContainerInfo containerInfo, boolean isFirst,
      boolean isLast) throws IOException {
    // JSON array format with proper brackets and commas
    if (isFirst) {
      // Start of array
      System.out.print("[");
    }
    
    // Print the container JSON
    System.out.print(WRITER.writeValueAsString(containerInfo));
    
    if (!isLast) {
      // Add comma between elements
      System.out.print(",");
    }
    
    if (isLast) {
      // End of array
      System.out.println("]");
    } else {
      // Add newline for readability
      System.out.println();
    }
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
      
      int totalSize = containerListAndTotalCount.getContainerInfoList().size();
      
      if (jsonFormat) {
        // JSON array format
        for (int i = 0; i < totalSize; i++) {
          ContainerInfo container = containerListAndTotalCount.getContainerInfoList().get(i);
          outputContainerInfoAsJsonMember(container, i == 0, i == totalSize - 1);
        }
      } else {
        // Original format - one JSON object per line
        for (ContainerInfo container : containerListAndTotalCount.getContainerInfoList()) {
          outputContainerInfo(container);
        }
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
      
      if (jsonFormat) {
        // JSON array format for all containers
        boolean isFirstContainer = true;
        
        // Start JSON array
        System.out.print("[");

        do {
          // Fetch containers in batches of 'batchSize'
          containerListAndTotalCount = scmClient.listContainer(currentStartId, batchSize, state, type, repConfig);
          fetchedCount = containerListAndTotalCount.getContainerInfoList().size();

          for (int i = 0; i < fetchedCount; i++) {
            ContainerInfo container = containerListAndTotalCount.getContainerInfoList().get(i);
            
            // Only the first container overall doesn't need a preceding comma
            if (!isFirstContainer) {
              System.out.print(",");
              System.out.println();
            }
            
            // Print the container JSON
            System.out.print(WRITER.writeValueAsString(container));
            isFirstContainer = false;
          }

          if (fetchedCount > 0) {
            currentStartId =
                containerListAndTotalCount.getContainerInfoList().get(fetchedCount - 1).getContainerID() + 1;
          }
        } while (fetchedCount > 0);
        
        // Close the JSON array
        System.out.println("]");
      } else {
        // Original format - one JSON object per line
        do {
          // Fetch containers in batches of 'batchSize'
          containerListAndTotalCount = scmClient.listContainer(currentStartId, batchSize, state, type, repConfig);
          fetchedCount = containerListAndTotalCount.getContainerInfoList().size();

          for (ContainerInfo container : containerListAndTotalCount.getContainerInfoList()) {
            outputContainerInfo(container);
          }

          if (fetchedCount > 0) {
            currentStartId =
                containerListAndTotalCount.getContainerInfoList().get(fetchedCount - 1).getContainerID() + 1;
          }
        } while (fetchedCount > 0);
      }
    }
  }
}

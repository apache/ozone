/**
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
package org.apache.hadoop.ozone.container.replication;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.stream.StreamingDestination;

/**
 * Streaming destination for closed-container replication.
 */
public class ContainerStreamingDestination implements StreamingDestination {

  private final KeyValueContainerData containerData;

  public ContainerStreamingDestination(KeyValueContainerData containerData) {
    this.containerData = containerData;
  }

  @Override
  public Path mapToDestination(String name) {
    if (name.equals("container.yaml")) {
      return Paths.get(
          containerData.getContainerFile().getAbsolutePath() + ".original");
    }

    String[] parts = name.split("/", 2);
    if (parts[0].equals("DB")) {
      return Paths.get(containerData.getContainerDBFile().getAbsolutePath(),
          parts[1]);
    } else if (parts[0].equals("DATA")) {
      return Paths.get(containerData.getChunksPath(),
          parts[1]);
    }
    throw new IllegalArgumentException("Unknown container part: " + parts[0]);
  }
}

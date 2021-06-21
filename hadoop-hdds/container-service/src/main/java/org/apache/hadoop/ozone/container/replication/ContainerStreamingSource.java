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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.stream.StreamingSource;

/**
 * Streaming source for closed-container replication.
 */
public class ContainerStreamingSource implements StreamingSource {

  private ContainerSet containerSet;

  public ContainerStreamingSource(ContainerSet containerSet) {
    this.containerSet = containerSet;
  }

  @Override
  public Map<String, Path> getFilesToStream(String id) {

    Map<String, Path> filesToStream = new HashMap<>();

    final long containerId = Long.parseLong(id);
    final KeyValueContainer container =
        (KeyValueContainer) containerSet.getContainer(containerId);

    try {
      container.release();
    } catch (StorageContainerException e) {
      throw new RuntimeException("Container couldn't be released: " + id, e);
    }

    if (container.getContainerState() != State.CLOSED
        && container.getContainerState() != State.QUASI_CLOSED) {
      throw new RuntimeException(
          "Only (quasi)closed containers can be exported, but ContainerId=: "
              + id + " is in state " + container.getContainerState());
    }

    try {

      final File dbPath =
          container.getContainerData().getContainerDBFile();

      final List<Path> dbFiles =
          Files.list(dbPath.toPath()).collect(Collectors.toList());
      for (Path dbFile : dbFiles) {
        if (dbFile.getFileName() != null) {
          filesToStream.put("DB/" + dbFile.getFileName(), dbFile);
        }
      }

      filesToStream.put("container.yaml",
          container.getContainerData().getContainerFile().toPath());

      final Path dataPath =
          Paths.get(container.getContainerData().getChunksPath());

      Files.walk(dataPath)
          .filter(Files::isRegularFile)
          .forEach(path -> {
            filesToStream
                .put("DATA/" + dataPath.relativize(path), path);
          });

    } catch (IOException e) {
      throw new RuntimeException("Couldn't stream countainer " + containerId,
          e);
    }
    return filesToStream;
  }
}

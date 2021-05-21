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
        filesToStream.put("DB/" + dbFile.getFileName().toString(), dbFile);
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

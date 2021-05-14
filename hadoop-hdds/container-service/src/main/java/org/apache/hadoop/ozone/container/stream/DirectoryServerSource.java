package org.apache.hadoop.ozone.container.stream;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class DirectoryServerSource implements StreamingSource {

  private Path root;

  public DirectoryServerSource(Path root) {
    this.root = root;
  }

  @Override
  public Map<String, Path> getFilesToStream(String id) {
    Map<String, Path> files = new HashMap<>();
    try {
      Files.walk(root.resolve(id))
          .filter(Files::isRegularFile)
          .forEach(path -> {
            files.put(root.relativize(path).toString(), path);
          });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return files;
  }
}

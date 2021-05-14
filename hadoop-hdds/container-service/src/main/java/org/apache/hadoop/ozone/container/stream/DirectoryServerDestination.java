package org.apache.hadoop.ozone.container.stream;

import java.nio.file.Path;
import java.nio.file.Paths;

public class DirectoryServerDestination implements StreamingDestination {

  private Path root;

  public DirectoryServerDestination(Path path) {
    root = path;
  }

  @Override
  public Path mapToDestination(String name) {
    return root.resolve(Paths.get(name));
  }
}

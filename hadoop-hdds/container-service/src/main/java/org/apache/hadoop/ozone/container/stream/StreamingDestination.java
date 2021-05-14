package org.apache.hadoop.ozone.container.stream;

import java.nio.file.Path;

public interface StreamingDestination {

  Path mapToDestination(String name);

}

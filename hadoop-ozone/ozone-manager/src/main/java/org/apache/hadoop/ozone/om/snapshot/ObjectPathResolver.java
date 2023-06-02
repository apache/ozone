package org.apache.hadoop.ozone.om.snapshot;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

public interface ObjectPathResolver {

  public Map<Long, Path> getAbsolutePathForObjectIDs(Set<Long> objIds)
      throws IOException;
}

package org.apache.hadoop.ozone.om.eventlistener;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;

public class LocalFileCheckpointStrategy implements NotificationCheckpointStrategy {

  private final Path path;

  public LocalFileCheckpointStrategy(Path path) {
    this.path = path;
  }

  public String load() throws IOException {
    try {
      List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
      if (!lines.isEmpty()) {
        return lines.get(0);
      }
    } catch (NoSuchFileException ex) {
      // assume no existing file
    }
    return null;
  }

  public void save(String val) throws IOException {
    Path tmpFile = Paths.get(path.toString() + "-" + System.currentTimeMillis());
    // Write to a temp file and atomic rename to avoid corrupting the
    // file if we are interrupted by a restart while in the middle of
    // writing
    Files.write(tmpFile, val.getBytes());
    Files.move(tmpFile, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
  }
}


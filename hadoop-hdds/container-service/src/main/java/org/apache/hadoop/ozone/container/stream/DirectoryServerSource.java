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

package org.apache.hadoop.ozone.container.stream;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Streaming files from single directory.
 */
public class DirectoryServerSource implements StreamingSource {

  private Path root;

  public DirectoryServerSource(Path root) {
    this.root = root;
  }

  @Override
  /**
   * Return logicalNames and real file path to replicate.
   *
   * @param id name of the subdirectory to replitace relative to root.
   */
  public Map<String, Path> getFilesToStream(String id)
      throws InterruptedException {
    Map<String, Path> files = new HashMap<>();
    final Path streamingDir = root.resolve(id);
    try (Stream<Path> list = Files.walk(streamingDir)
            .filter(Files::isRegularFile)) {
      list.forEach(path -> {
        files.put(root.relativize(path).toString(), path);
      });
    } catch (IOException e) {
      throw new StreamingException("Couldn't read directory for streaming: " +
          streamingDir, e);
    }
    return files;
  }
}

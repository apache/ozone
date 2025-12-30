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

package org.apache.hadoop.ozone.freon;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Optional;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import picocli.CommandLine.Option;

/**
 * Base class for Freon generator tests that requires  {@link FileSystem} instance.
 */
public abstract class HadoopBaseFreonGenerator extends BaseFreonGenerator {

  @Option(names = {"-r", "--rpath", "--path"},
      description = "Hadoop FS file system path. Use full path.",
      defaultValue = "o3fs://bucket1.vol1")
  private String rootPath;

  private final ThreadLocal<FileSystem>  threadLocalFileSystem =
      ThreadLocal.withInitial(this::createFS);

  private OzoneConfiguration configuration;
  private URI uri;

  @Override
  public void init() {
    super.init();
    configuration = createOzoneConfiguration();
    uri = URI.create(rootPath);
    String scheme = Optional.ofNullable(uri.getScheme())
        .orElseGet(() -> FileSystem.getDefaultUri(configuration)
            .getScheme());
    String disableCacheName =
        String.format("fs.%s.impl.disable.cache", scheme);
    print("Disabling FS cache: " + disableCacheName);
    configuration.setBoolean(disableCacheName, true);
  }

  @Override
  protected void taskLoopCompleted() {
    FileSystem fileSystem = threadLocalFileSystem.get();
    try {
      fileSystem.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected String getRootPath() {
    return rootPath;
  }

  protected FileSystem getFileSystem() {
    return threadLocalFileSystem.get();
  }

  private FileSystem createFS() {
    try {
      return FileSystem.get(uri, configuration);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

}

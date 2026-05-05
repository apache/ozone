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

package org.apache.hadoop.hdds.server;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import org.apache.ratis.util.AtomicFileOutputStream;
import org.slf4j.Logger;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.inspector.TagInspector;

/**
 * YAML utilities.
 */
public final class YamlUtils {

  private static final Yaml LOADER = getYamlForLoad();

  private YamlUtils() {
    // no instances
  }

  public static <T> T loadAs(InputStream input, Class<? super T> type) {
    return LOADER.loadAs(input, type);
  }

  private static Yaml getYamlForLoad() {
    TagInspector tags = tag -> tag.getClassName().startsWith("org.apache.hadoop.hdds.")
        || tag.getClassName().startsWith("org.apache.hadoop.ozone.");
    LoaderOptions loaderOptions = new LoaderOptions();
    loaderOptions.setTagInspector(tags);
    return new Yaml(loaderOptions);
  }

  public static void dump(Yaml yaml, Object data, File file, Logger log) throws IOException {
    try (OutputStream out = new AtomicFileOutputStream(file);
         OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
      yaml.dump(data, writer);
    } catch (IOException e) {
      log.warn("Failed to dump {}", data, e);
      throw e;
    }
  }
}

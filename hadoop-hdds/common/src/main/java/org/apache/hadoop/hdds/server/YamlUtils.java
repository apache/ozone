/*
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
package org.apache.hadoop.hdds.server;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.inspector.TagInspector;
import org.yaml.snakeyaml.inspector.TrustedPrefixesTagInspector;

import java.io.InputStream;
import java.util.Arrays;

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
    TagInspector tags = new TrustedPrefixesTagInspector(Arrays.asList(
        "org.apache.hadoop.ozone.", "org.apache.hadoop.hdds."));
    LoaderOptions loaderOptions = new LoaderOptions();
    loaderOptions.setTagInspector(tags);
    return new Yaml(loaderOptions);
  }
}

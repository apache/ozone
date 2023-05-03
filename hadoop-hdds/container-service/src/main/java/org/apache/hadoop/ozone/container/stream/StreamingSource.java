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
package org.apache.hadoop.ozone.container.stream;

import java.nio.file.Path;
import java.util.Map;

/**
 * Interface to define which files should be replicated for a given id.
 */
public interface StreamingSource {

  /**
   *
   * @param id: custom identifier
   *
   * @return map of files which should be copied (logical name -> real path)
   */
  Map<String, Path> getFilesToStream(String id) throws InterruptedException;

}

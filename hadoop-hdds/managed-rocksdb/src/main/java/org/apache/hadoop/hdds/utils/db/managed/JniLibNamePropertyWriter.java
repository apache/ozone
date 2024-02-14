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
package org.apache.hadoop.hdds.utils.db.managed;

import org.rocksdb.util.Environment;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Class to write the rocksdb lib name to a file.
 * This would be used to build native ozone_rocksdb_tools library.
 */
public final class JniLibNamePropertyWriter {

  private JniLibNamePropertyWriter() {
  }

  public static void main(String[] args) {
    String filePath = args[0];
    try (Writer writer = new OutputStreamWriter(
        Files.newOutputStream(Paths.get(filePath)), StandardCharsets.UTF_8)) {
      writer.write("rocksdbLibName=" +
          Environment.getJniLibraryFileName("rocksdb"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

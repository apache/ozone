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

import org.apache.hadoop.hdds.conf.StorageSize;
import org.apache.hadoop.hdds.conf.StorageUnit;
import picocli.CommandLine.ITypeConverter;

/**
 * A Picocli custom converter for parsing command line string values into
 * StorageSize objects.
 */

public class StorageSizeConverter implements ITypeConverter<StorageSize> {

  public static final String STORAGE_SIZE_DESCRIPTION = "You can specify the " +
      "size using data units like 'GB', 'MB', 'KB', etc. Size is in base 2 " +
      "binary.";

  @Override
  public StorageSize convert(String value) {
    return StorageSize.parse(value, StorageUnit.BYTES);
  }
}

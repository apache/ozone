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

package org.apache.hadoop.ozone.shell;

import org.apache.hadoop.ozone.security.acl.OzoneObj;
import picocli.CommandLine;

/**
 * Option for {@link org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType}.
 */
public class StoreTypeOption
    implements CommandLine.ITypeConverter<OzoneObj.StoreType> {

  @CommandLine.Option(names = {"--store", "-s"},
      description = "Store type. i.e OZONE or S3",
      defaultValue = "OZONE",
      converter = StoreTypeOption.class
  )
  private OzoneObj.StoreType value;

  public OzoneObj.StoreType getValue() {
    return value;
  }

  @Override
  public OzoneObj.StoreType convert(String str) {
    return str != null
        ? OzoneObj.StoreType.valueOf(str)
        : OzoneObj.StoreType.OZONE;
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.web.ozShell.s3;

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.web.ozShell.OzoneAddress;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.IOException;

/**
 * S3Bucket mapping handler, which returns volume name and Ozone fs uri for
 * that bucket.
 */
@Command(name = "path",
    description = "Returns the ozone path for S3Bucket")
public class S3BucketMapping extends S3Handler {

  @Parameters(arity = "1..1", description = "Name of the s3 bucket.")
  private String s3BucketName;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    ObjectStore objectStore = client.getObjectStore();
    String mapping = objectStore.getOzoneBucketMapping(s3BucketName);
    String volumeName = objectStore.getOzoneVolumeName(s3BucketName);

    if (isVerbose()) {
      out().printf("Mapping created for S3Bucket is : %s%n", mapping);
    }

    out().printf("Volume name for S3Bucket is : %s%n", volumeName);

    String ozoneFsUri = String.format("%s://%s.%s", OzoneConsts
        .OZONE_URI_SCHEME, s3BucketName, volumeName);

    out().printf("Ozone FileSystem Uri is : %s%n", ozoneFsUri);
  }
}

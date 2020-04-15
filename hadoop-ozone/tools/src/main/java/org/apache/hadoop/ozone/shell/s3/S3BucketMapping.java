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
package org.apache.hadoop.ozone.shell.s3;

import org.apache.hadoop.ozone.OzoneConsts;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;

import static org.apache.hadoop.ozone.OzoneConsts.S3_VOLUME_NAME;

/**
 * S3Bucket mapping handler, which returns volume name and Ozone fs uri for
 * that bucket.
 */
@Command(name = "path",
    description = "Returns the ozone path for S3Bucket")
@SuppressWarnings("squid:S106") // CLI
public class S3BucketMapping implements Callable<Void> {

  @Parameters(arity = "1..1", description = "Name of the s3 bucket.")
  private String s3BucketName;

  @Override
  public Void call() {
    System.out.printf("Volume name for S3Bucket is : %s%n", S3_VOLUME_NAME);

    String ozoneFsUri = String.format("%s://%s.%s", OzoneConsts
        .OZONE_URI_SCHEME, s3BucketName, S3_VOLUME_NAME);

    System.out.printf("Ozone FileSystem Uri is : %s%n", ozoneFsUri);

    return null;
  }
}

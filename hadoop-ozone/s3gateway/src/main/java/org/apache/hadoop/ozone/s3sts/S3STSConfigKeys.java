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

package org.apache.hadoop.ozone.s3sts;

import org.apache.hadoop.ozone.OzoneConfigKeys;

/**
 * This class contains constants for configuration keys used
 * in S3 STS endpoint.
 */
public final class S3STSConfigKeys {
  public static final String OZONE_S3G_STS_HTTP_ENABLED_KEY =
      OzoneConfigKeys.OZONE_S3G_STS_HTTP_ENABLED_KEY;
  public static final String OZONE_S3G_STS_HTTP_BIND_HOST_KEY =
      "ozone.s3g.sts.http-bind-host";
  public static final String OZONE_S3G_STS_HTTPS_BIND_HOST_KEY =
      "ozone.s3g.sts.https-bind-host";
  public static final String OZONE_S3G_STS_HTTP_ADDRESS_KEY =
      "ozone.s3g.sts.http-address";
  public static final String OZONE_S3G_STS_HTTPS_ADDRESS_KEY =
      "ozone.s3g.sts.https-address";
  public static final int OZONE_S3G_STS_HTTP_BIND_PORT_DEFAULT = 9880;
  public static final int OZONE_S3G_STS_HTTPS_BIND_PORT_DEFAULT = 9881;
  // Max payload default size for STS AssumeRole API calls (32 KB)
  // as STS AssumeRole has these parameters required in payload:
  // Action=AssumeRole&RoleArn=...&RoleSessionName=...&DurationSeconds=...
  // where RoleArn max length is 2048 and max bytes per character in UTF-8 encoding is 12
  // (2048 * 12 = 24576) + other parameters and overheads, so setting to 32 KB
  // this limit can be adjusted via configuration if needed.
  public static final int OZONE_S3G_STS_PAYLOAD_HASH_MAX_VALUE = 32768;

  /**
   * Never constructed.
   */
  private S3STSConfigKeys() {

  }
}

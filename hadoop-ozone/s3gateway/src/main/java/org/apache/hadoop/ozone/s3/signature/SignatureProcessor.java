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

package org.apache.hadoop.ozone.s3.signature;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.time.format.DateTimeFormatter;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

/**
 * Parser to request auth parser for http request.
 */
public interface SignatureProcessor {

  String CONTENT_TYPE = "content-type";

  String CONTENT_MD5 = "content-md5";

  String AWS4_SIGNING_ALGORITHM = "AWS4-HMAC-SHA256";

  String HOST_HEADER = "Host";

  DateTimeFormatter DATE_FORMATTER =
      DateTimeFormatter.ofPattern("yyyyMMdd");

  /**
   * API to return string to sign.
   */
  SignatureInfo parseSignature() throws OS3Exception, IOException, NoSuchAlgorithmException;
}

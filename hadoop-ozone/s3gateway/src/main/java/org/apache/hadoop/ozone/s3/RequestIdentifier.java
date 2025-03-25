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

package org.apache.hadoop.ozone.s3;

import java.security.SecureRandom;
import java.util.Random;
import javax.enterprise.context.RequestScoped;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;

/**
 * Request specific identifiers.
 */
@RequestScoped
public class RequestIdentifier {

  private static final Random RANDOM = new SecureRandom();

  private final String requestId;

  private final String amzId;

  public RequestIdentifier() {
    int count = 8 + RANDOM.nextInt(8);
    amzId = RandomStringUtils.random(count, 0, 0, true, true, null, RANDOM);
    requestId = OzoneUtils.getRequestID();
  }

  public String getRequestId() {
    return requestId;
  }

  public String getAmzId() {
    return amzId;
  }
}

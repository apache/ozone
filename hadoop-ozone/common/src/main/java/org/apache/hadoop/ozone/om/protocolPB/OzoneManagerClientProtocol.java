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

package org.apache.hadoop.ozone.om.protocolPB;

import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocol.S3Auth;

/**
 * OzoneManagerClientProtocol defines interfaces needed on the client side
 * when communicating with Ozone Manager.
 */
public interface OzoneManagerClientProtocol extends OzoneManagerProtocol {
  /**
   * Sets the S3 Authentication information when OM request is generated as
   * part of the S3 API implementation from Ozone. S3 Gateway needs to add
   * authentication information on a per-request basis which is attached as
   * a thread local variable.
   */
  void setThreadLocalS3Auth(S3Auth s3Auth);

  S3Auth getThreadLocalS3Auth();

  void clearThreadLocalS3Auth();

  ThreadLocal<S3Auth> getS3CredentialsProvider();
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ipc_;

import java.io.IOException;


/**
 * Exception thrown by a server typically to indicate that server is in a state
 * where request cannot be processed temporarily (such as still starting up).
 * Client may retry the request. If the service is up, the server may be able to
 * process a retried request.
 */
public class RetriableException extends IOException {
  private static final long serialVersionUID = 1915561725516487301L;
  
  public RetriableException(Exception e) {
    super(e);
  }
  
  public RetriableException(String msg) {
    super(msg);
  }
}

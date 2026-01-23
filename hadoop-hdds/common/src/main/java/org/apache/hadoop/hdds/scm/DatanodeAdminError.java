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

package org.apache.hadoop.hdds.scm;

/**
 * Simple class to wrap a datanode admin host and error message.
 */
public class DatanodeAdminError {

  private String hostname;
  private String error;

  public DatanodeAdminError(String host, String error) {
    this.hostname = host;
    this.error = error;
  }

  public String getHostname() {
    return hostname;
  }

  public String getError() {
    return error;
  }

}

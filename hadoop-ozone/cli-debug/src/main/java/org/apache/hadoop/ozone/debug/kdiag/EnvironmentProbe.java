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

package org.apache.hadoop.ozone.debug.kdiag;

/**
 * Prints environment variables relevant to Kerberos and Ozone.
 */
public class EnvironmentProbe implements DiagnosticProbe {

  @Override
  public String name() {
    return "Environment Variables";
  }

  @Override
  public boolean run() {
    System.out.println("-- Environment Variables --");
    print("KRB5_CONFIG");
    print("KRB5CCNAME");
    print("OZONE_CONF_DIR");
    print("HADOOP_CONF_DIR");
    print("JAVA_SECURITY_KRB5_CONF");
    return true;
  }

  private void print(String key) {
    String value = System.getenv(key);
    System.out.println(key + " = "
        + (value == null ? "(unset)" : value));
  }
}

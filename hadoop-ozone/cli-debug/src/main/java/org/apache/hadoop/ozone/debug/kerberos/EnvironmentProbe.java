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

package org.apache.hadoop.ozone.debug.kerberos;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;

/**
 * Prints environment variables relevant to Kerberos and Ozone.
 */
public class EnvironmentProbe extends ConfigProbe {

  @Override
  public String name() {
    return "Environment Variables";
  }

  @Override
  public ProbeResult test(OzoneConfiguration conf) {

    printValue("KRB5_CONFIG", System.getenv("KRB5_CONFIG"));
    printValue("KRB5CCNAME", System.getenv("KRB5CCNAME"));
    printValue("OZONE_CONF_DIR", System.getenv("OZONE_CONF_DIR"));
    printValue("HADOOP_CONF_DIR", System.getenv("HADOOP_CONF_DIR"));
    printValue("JAVA_SECURITY_KRB5_CONF", System.getenv("JAVA_SECURITY_KRB5_CONF"));

    return ProbeResult.PASS;
  }
}

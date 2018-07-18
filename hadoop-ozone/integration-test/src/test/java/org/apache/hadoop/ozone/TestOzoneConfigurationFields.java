/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import org.apache.hadoop.conf.TestConfigurationFieldsBase;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;

/**
 * Tests if configuration constants documented in ozone-defaults.xml.
 */
public class TestOzoneConfigurationFields extends TestConfigurationFieldsBase {

  @Override
  public void initializeMemberVariables() {
    xmlFilename = new String("ozone-default.xml");
    configurationClasses =
        new Class[] {OzoneConfigKeys.class, ScmConfigKeys.class,
            OMConfigKeys.class, HddsConfigKeys.class};
    errorIfMissingConfigProps = true;
    errorIfMissingXmlProps = true;
    xmlPropsToSkipCompare.add("hadoop.tags.custom");
  }
}
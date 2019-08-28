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
package org.apache.hadoop.hdds.discovery;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

/**
 * JAXB representation of Hadoop Configuration.
 */
@XmlRootElement(name = "configuration")
public class ConfigurationXml {

  private List<ConfigurationXmlEntry> property = new ArrayList<>();

  public List<ConfigurationXmlEntry> getProperty() {
    return property;
  }

  public void setProperty(
      List<ConfigurationXmlEntry> property) {
    this.property = property;
  }

  public void addConfiguration(String key, String name) {
    property.add(new ConfigurationXmlEntry(key, name));
  }
}

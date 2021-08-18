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

package org.apache.hadoop.hdds.cli;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReconfigurationUtil {

  public static class PropertyChange {
    public String prop;
    public String oldVal;
    public String newVal;

    public PropertyChange(String prop, String newVal, String oldVal) {
      this.prop = prop;
      this.newVal = newVal;
      this.oldVal = oldVal;
    }
  }

  public static Collection<PropertyChange>
      getChangedProperties(List<OzoneConfiguration.Property> newConf,
                       List<OzoneConfiguration.Property> oldConf) {
    Map<String, PropertyChange> changes = new HashMap<String, PropertyChange>();

    // iterate over old configuration
    for (OzoneConfiguration.Property oldEntry: oldConf) {
      String oldkey = oldEntry.getName();
      String oldVal = oldEntry.getValue();
      String newVal = null;
      for (OzoneConfiguration.Property newEntry: newConf) {
        if (newEntry.getName().equals(oldkey)) {
          newVal = newEntry.getValue();
        }
      }
      if (newVal == null || !newVal.equals(oldVal)) {
        changes.put(oldkey, new PropertyChange(oldkey, newVal, oldVal));
      }
    }

    // now iterate over new configuration
    // (to look for properties not present in old conf)
    List<String> oldKeys = new ArrayList<>();
    for (OzoneConfiguration.Property oldEntry: oldConf) {
      oldKeys.add(oldEntry.getName());
    }
    for (OzoneConfiguration.Property newEntry: newConf) {
      String newkey = newEntry.getName();
      String newVal = newEntry.getValue();
      if (!oldKeys.contains(newkey)) {
        changes.put(newkey, new PropertyChange(newkey, newVal, null));
      }
    }

    return changes.values();
  }

  public Collection<PropertyChange> parseChangedProperties(
      List<OzoneConfiguration.Property> newConf,
      List<OzoneConfiguration.Property> oldConf) {
    return getChangedProperties(newConf, oldConf);
  }
}
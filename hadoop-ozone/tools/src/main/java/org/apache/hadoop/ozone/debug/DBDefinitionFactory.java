/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.debug;

import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;

import java.util.HashMap;

/**
 * Utility class to get appropriate DBDefinition.
 */
public final class DBDefinitionFactory {

  private DBDefinitionFactory() {
  }

  private static HashMap<String, DBDefinition> dbMap;

  static {
    dbMap = new HashMap<>();
    dbMap.put(new SCMDBDefinition().getName(), new SCMDBDefinition());
    dbMap.put(new OMDBDefinition().getName(), new OMDBDefinition());
  }

  public static DBDefinition getDefinition(String dbName){
    if (dbMap.containsKey(dbName)){
      return dbMap.get(dbName);
    }
    return null;
  }
}

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

package org.apache.hadoop.ozone.containerlog.parser;

/**
 * Constants used for ContainerDatanodeDatabase.
 */
public final class DBConsts {

  private DBConsts() {
    //Never constructed
  }

  public static final String DRIVER = "org.sqlite.JDBC";
  public static final String CONNECTION_PREFIX = "jdbc:sqlite:";
  public static final String DATABASE_NAME = "container_datanode.db";
  public static final String PROPS_FILE = "container-log-db-queries.properties";
  public static final int CACHE_SIZE = 1000000;
  public static final int BATCH_SIZE = 1000;
  public static final String DATANODE_CONTAINER_LOG_TABLE_NAME = "DatanodeContainerLogTable";
  public static final String CONTAINER_LOG_TABLE_NAME = "ContainerLogTable";

}

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

package org.apache.hadoop.ozone.repair.om;

import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.common.FSOBaseTool;

import java.io.IOException;

/**
 * Tool to identify and repair disconnected FSO trees in all buckets.
 */
public class FSORepairTool extends FSOBaseTool {

  public FSORepairTool(String dbPath, boolean dryRun) throws IOException {
    this(getStoreFromPath(dbPath), dbPath, dryRun);
  }

  public FSORepairTool(DBStore dbStore, String dbPath, boolean dryRun) throws IOException {
    super(dbStore, dbPath, dryRun);
  }

}

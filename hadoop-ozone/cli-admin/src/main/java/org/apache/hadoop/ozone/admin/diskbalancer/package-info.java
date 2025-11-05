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

/**
 * DiskBalancer CLI commands for direct client-to-datanode operations.
 * <p>
 * This package contains command-line interface implementations for managing
 * DiskBalancer operations on Ozone datanodes. Commands communicate directly
 * with datanodes via RPC.
 * <p>
 * Available commands:
 * <ul>
 *   <li>start - Start DiskBalancer on specified datanodes</li>
 *   <li>stop - Stop DiskBalancer on specified datanodes</li>
 *   <li>update - Update DiskBalancer configuration on datanodes</li>
 *   <li>report - Get volume density reports from datanodes</li>
 *   <li>status - Get DiskBalancer status from datanodes</li>
 * </ul>
 */
package org.apache.hadoop.ozone.admin.diskbalancer;


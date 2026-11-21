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
 * Deterministic simulation of SCM: the real StorageContainerManager runs on one thread against simulated datanodes,
 * with a simulated clock and seeded randomness, while faults are injected and invariants are checked. Start with
 * {@link org.apache.hadoop.hdds.scm.simulation.ScmSimulation} and
 * {@link org.apache.hadoop.hdds.scm.simulation.TestScmSimulation}.
 */
package org.apache.hadoop.hdds.scm.simulation;

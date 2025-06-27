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

package org.apache.hadoop.ozone.metrics;

/**
 * The class was added in order to use it later for non-blocking metrics as it was not possible
 * to inherit from the existing ones.
 * A version of {@link org.apache.hadoop.metrics2.lib.MutableRate}.
 */
public class MutableRate extends MutableStat {

  public MutableRate(String name, String description, boolean extended) {
    super(name, description, "Ops", "Time", extended);
  }
}

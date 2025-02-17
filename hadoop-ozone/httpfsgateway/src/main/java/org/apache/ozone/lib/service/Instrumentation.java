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

package org.apache.ozone.lib.service;

import java.util.Map;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;

/**
 * Hadoop server instrumentation implementation.
 */
@InterfaceAudience.Private
public interface Instrumentation {

  /**
   * Cron interface.
   */
  interface Cron {

    Cron start();

    Cron stop();
  }

  /**
   * Variable interface.
   *
   * @param <T> the type of the variable.
   */
  interface Variable<T> {

    T getValue();
  }

  Cron createCron();

  void incr(String group, String name, long count);

  void addCron(String group, String name, Cron cron);

  void addVariable(String group, String name, Variable<?> variable);

  //sampling happens once a second
  void addSampler(String group,
                  String name,
                  int samplingSize,
                  Variable<Long> variable);

  Map<String, Map<String, ?>> getSnapshot();

}

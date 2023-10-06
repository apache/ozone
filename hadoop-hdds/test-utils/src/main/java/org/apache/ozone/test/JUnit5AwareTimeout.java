/*
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
package org.apache.ozone.test;

import org.junit.rules.Timeout;

/**
 * Disables the given JUnit4 timeout rule if JUnit5-specific timeout-mode is set
 * to "disabled".
 */
public class JUnit5AwareTimeout extends DisableOnProperty {

  public JUnit5AwareTimeout(Timeout delegate) {
    super(delegate, "junit.jupiter.execution.timeout.mode", "disabled");
  }

}

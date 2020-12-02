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

package org.apache.hadoop.ozone.om.upgrade;

import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.ERASURE_CODING;

/**
 * Test util class. To be removed.
 */
public class OMLayoutFeatureUtil {

  /**
   * This is an example of an "API" that uses a new Layout feature (EC) that is
   * not yet supported by the current layout version. The following can be
   * "disallowed" by just adding the following annotation, thereby keeping the
   * method logic and upgrade logic separate.
   */
  @DisallowedUntilLayoutVersion(ERASURE_CODING)
  public String ecMethod() {
    // Blah Blah EC Blah....
    return "ec";
  }

  /**
   * This is an example of an "API" that is
   * supported by the current layout version.
   */
  public String basicMethod() {
    // Blah Blah Basic Blah....
    return "basic";
  }
}


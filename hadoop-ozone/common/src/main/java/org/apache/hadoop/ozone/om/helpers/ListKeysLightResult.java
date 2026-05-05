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

package org.apache.hadoop.ozone.om.helpers;

import java.util.List;

/**
 * Encapsulates the result of listKeys. It contains a list of
 * {@link BasicOmKeyInfo} and a boolean flag indicating if the listing is
 * truncated.
 */
public class ListKeysLightResult {
  private List<BasicOmKeyInfo> keys;
  private boolean isTruncated;

  public ListKeysLightResult(List<BasicOmKeyInfo> keys, boolean isTruncated) {
    this.keys = keys;
    this.isTruncated = isTruncated;
  }

  public List<BasicOmKeyInfo> getKeys() {
    return keys;
  }

  public boolean isTruncated() {
    return isTruncated;
  }
}

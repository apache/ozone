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

package org.apache.hadoop.hdds.scm;

import java.util.Objects;

/**
 * Status of SCM safe mode exit rule.
 */
public final class SafeModeRuleStatus {

  private final boolean validated;
  private final String statusText;

  public SafeModeRuleStatus(boolean validated, String statusText) {
    this.validated = validated;
    this.statusText = statusText;
  }

  public boolean isValidated() {
    return validated;
  }

  public String getStatusText() {
    return statusText;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof SafeModeRuleStatus)) {
      return false;
    }
    SafeModeRuleStatus that = (SafeModeRuleStatus) other;
    return validated == that.validated
        && Objects.equals(statusText, that.statusText);
  }

  @Override
  public int hashCode() {
    return Objects.hash(validated, statusText);
  }

  @Override
  public String toString() {
    return "SafeModeRuleStatus{"
        + "validated=" + validated
        + ", statusText='" + statusText + '\''
        + '}';
  }
}

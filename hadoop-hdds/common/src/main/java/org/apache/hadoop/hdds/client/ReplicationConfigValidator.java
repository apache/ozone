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

package org.apache.hadoop.hdds.client;

import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.PostConstruct;

/**
 * Validator to check if replication config is enabled.
 */
@ConfigGroup(prefix = "ozone.replication")
public class ReplicationConfigValidator {

  @Config(key = "ozone.replication.allowed-configs",
      defaultValue = "^((STANDALONE|RATIS)/(ONE|THREE))|"
          + "(EC/(3-2|6-3|10-4)-(512|1024|2048|4096)k)"
          + "$",
      type = ConfigType.STRING,
      description = "Regular expression to restrict enabled " +
          "replication schemes",
      tags = ConfigTag.STORAGE)
  private String validationPattern;

  private Pattern compiledValidationPattern;

  public void disableValidation() {
    setValidationPattern("");
  }

  public void setValidationPattern(String pattern) {
    if (!Objects.equals(pattern, validationPattern)) {
      validationPattern = pattern;
      compilePattern();
    }
  }

  @PostConstruct
  public void init() {
    compilePattern();
  }

  private void compilePattern() {
    if (validationPattern != null && !validationPattern.equals("")) {
      compiledValidationPattern = Pattern.compile(validationPattern);
    } else {
      compiledValidationPattern = null;
    }
  }

  public ReplicationConfig validate(ReplicationConfig replicationConfig) {
    if (compiledValidationPattern == null) {
      return replicationConfig;
    }
    String input = replicationConfig.configFormat();
    if (!compiledValidationPattern.matcher(input).matches()) {
      throw new IllegalArgumentException("Invalid replication config " +
          input + ". Config must match the pattern defined by " +
          "ozone.replication.allowed-configs (" + validationPattern + ")");
    }
    return replicationConfig;
  }

}

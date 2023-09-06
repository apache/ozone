/**
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
package org.apache.hadoop.hdds.conf;

import static org.apache.hadoop.hdds.conf.TimeDurationUtil.getTimeDurationHelper;

/**
 * Possible type of injected configuration.
 * <p>
 * AUTO means that the exact type will be identified based on the java type of
 * the configuration field.
 */
public enum ConfigType {
  AUTO {
    @Override
    Object parse(String value, Config config, Class<?> type, String key) {
      throw new UnsupportedOperationException();
    }

  },
  STRING {
    @Override
    String parse(String value, Config config, Class<?> type, String key) {
      return value;
    }

  },
  BOOLEAN {
    @Override
    Boolean parse(String value, Config config, Class<?> type, String key) {
      return Boolean.parseBoolean(value);
    }

  },
  INT {
    @Override
    Integer parse(String value, Config config, Class<?> type, String key) {
      return Integer.parseInt(value);
    }

  },
  LONG {
    @Override
    Long parse(String value, Config config, Class<?> type, String key) {
      return Long.parseLong(value);
    }

  },
  TIME {
    @Override
    Long parse(String value, Config config, Class<?> type, String key) {
      return getTimeDurationHelper(key, value, config.timeUnit());
    }

  },
  SIZE {
    @Override
    Object parse(String value, Config config, Class<?> type, String key) {
      StorageSize measure = StorageSize.parse(value);
      long val = Math.round(measure.getUnit().toBytes(measure.getValue()));
      if (type == int.class) {
        return (int) val;
      }
      return val;
    }

  },
  CLASS {
    @Override
    Class<?> parse(String value, Config config, Class<?> type, String key)
        throws ClassNotFoundException {
      return Class.forName(value);
    }

  },
  DOUBLE {
    @Override
    Double parse(String value, Config config, Class<?> type, String key) {
      return Double.parseDouble(value);
    }

  };

  abstract Object parse(String value, Config config, Class<?> type, String key)
      throws Exception;
}

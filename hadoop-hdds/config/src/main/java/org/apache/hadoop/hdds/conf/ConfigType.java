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

package org.apache.hadoop.hdds.conf;

import static org.apache.hadoop.hdds.conf.TimeDurationUtil.getDuration;
import static org.apache.hadoop.hdds.conf.TimeDurationUtil.getTimeDurationHelper;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

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

    @Override
    void set(ConfigurationTarget target, String key, Object value,
        Config config) {
      throw new UnsupportedOperationException();
    }
  },
  STRING {
    @Override
    String parse(String value, Config config, Class<?> type, String key) {
      return value;
    }

    @Override
    void set(ConfigurationTarget target, String key, Object value,
        Config config) {
      target.set(key, String.valueOf(value));
    }
  },
  BOOLEAN {
    @Override
    Boolean parse(String value, Config config, Class<?> type, String key) {
      return Boolean.parseBoolean(value);
    }

    @Override
    void set(ConfigurationTarget target, String key, Object value,
        Config config) {
      target.setBoolean(key, (boolean) value);
    }
  },
  INT {
    @Override
    Integer parse(String value, Config config, Class<?> type, String key) {
      return Integer.parseInt(value);
    }

    @Override
    void set(ConfigurationTarget target, String key, Object value,
        Config config) {
      target.setInt(key, (int) value);
    }
  },
  LONG {
    @Override
    Long parse(String value, Config config, Class<?> type, String key) {
      return Long.parseLong(value);
    }

    @Override
    void set(ConfigurationTarget target, String key, Object value,
        Config config) {
      target.setLong(key, (long) value);
    }
  },
  TIME {
    @Override
    Object parse(String value, Config config, Class<?> type, String key) {
      if (type == Duration.class) {
        return getDuration(key, value, config.timeUnit());
      }
      return getTimeDurationHelper(key, value, config.timeUnit());
    }

    @Override
    void set(ConfigurationTarget target, String key, Object value,
        Config config) {
      if (value instanceof Duration) {
        final Duration duration = (Duration) value;
        if (duration.getNano() % 1_000_000 > 0) {
          target.setTimeDuration(key, duration.toNanos(), TimeUnit.NANOSECONDS);
        } else {
          final long millis = duration.toMillis();
          target.setTimeDuration(key, millis, TimeUnit.MILLISECONDS);
        }
      } else {
        target.setTimeDuration(key, (long) value, config.timeUnit());
      }
    }
  },
  SIZE {
    @Override
    Object parse(String value, Config config, Class<?> type, String key) {
      StorageSize measure = StorageSize.parse(value, config.sizeUnit());
      long val = Math.round(measure.getUnit().toBytes(measure.getValue()));
      if (type == int.class) {
        return (int) val;
      }
      return val;
    }

    @Override
    void set(ConfigurationTarget target, String key, Object value,
        Config config) {
      if (value instanceof Long) {
        target.setStorageSize(key, (long) value, config.sizeUnit());
      } else if (value instanceof Integer) {
        target.setStorageSize(key, (int) value, config.sizeUnit());
      } else {
        throw new ConfigurationException("Unsupported type " + value.getClass()
            + " for " + key);
      }
    }
  },
  CLASS {
    @Override
    Class<?> parse(String value, Config config, Class<?> type, String key)
        throws ClassNotFoundException {
      return Class.forName(value);
    }

    @Override
    void set(ConfigurationTarget target, String key, Object value,
        Config config) {
      if (value instanceof Class<?>) {
        target.set(key, ((Class<?>) value).getName());
      } else {
        throw new ConfigurationException("Unsupported type " + value.getClass()
            + " for " + key);
      }
    }
  },
  DOUBLE {
    @Override
    Double parse(String value, Config config, Class<?> type, String key) {
      return Double.parseDouble(value);
    }

    @Override
    void set(ConfigurationTarget target, String key, Object value,
        Config config) {
      target.setDouble(key, (double) value);
    }
  },
  FLOAT {
    @Override
    Float parse(String value, Config config, Class<?> type, String key) {
      return Float.parseFloat(value);
    }

    @Override
    void set(ConfigurationTarget target, String key, Object value,
        Config config) {
      target.setFloat(key, (float) value);
    }
  };

  abstract Object parse(String value, Config config, Class<?> type, String key)
      throws Exception;

  abstract void set(ConfigurationTarget target, String key, Object value,
      Config config);
}

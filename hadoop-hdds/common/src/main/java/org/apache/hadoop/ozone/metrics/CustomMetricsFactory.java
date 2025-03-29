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

import java.lang.reflect.Field;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.DefaultMetricsFactory;
import org.apache.hadoop.metrics2.lib.MutableMetric;
import org.apache.hadoop.metrics2.lib.MutableMetricsFactory;

/**
 * Custom factory to create the objects to measure cluster metrics.
 */
public class CustomMetricsFactory
    extends MutableMetricsFactory {

  private static final MutableMetricsFactory INSTANCE =
      new CustomMetricsFactory();

  /**
   * Get {@link MutableMetricsFactory} instance.
   * @return {@link MutableMetricsFactory} instance
   */
  public static MutableMetricsFactory getInstance() {
    return INSTANCE;
  }

  /**
   * Register as factory for {@link DefaultMetricsFactory} instance.
   */
  public static void registerAsDefaultMutableMetricsFactory() {
    DefaultMetricsFactory.INSTANCE.setInstance(INSTANCE);
  }

  @Override
  protected MutableMetric newForField(Field field, Metric annotation) {
    MetricsInfo info = getInfo(annotation, field);
    final Class<?> cls = field.getType();
    if (cls == MutableStat.class) {
      return new MutableStat(info.name(), info.description(), annotation.sampleName(), annotation.valueName(),
          annotation.always());
    }

    if (cls == MutableRate.class) {
      return new MutableRate(info.name(), info.description(),
          annotation.always());
    }
    if (cls == MutableQuantiles.class) {
      return new MutableQuantiles(info.name(), annotation.about(),
          annotation.sampleName(), annotation.valueName(),
          annotation.interval());
    }
    return null;
  }

}

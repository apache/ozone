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

package org.apache.hadoop.ozone.insight;

import java.lang.reflect.Field;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.insight.Component.Type;
import picocli.CommandLine;

/**
 * Subcommand to show configuration values/documentation.
 */
@CommandLine.Command(
    name = "config",
    description = "Show configuration for a specific subcomponents",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ConfigurationSubCommand extends BaseInsightSubCommand
    implements Callable<Void> {

  @CommandLine.Parameters(description = "Name of the insight point (use list "
      + "to check the available options)")
  private String insightName;

  @Override
  public Void call() throws Exception {
    InsightPoint insight =
        getInsight(getInsightCommand().getOzoneConf(), insightName);
    System.out.println(
        "Configuration for `" + insightName + "` (" + insight.getDescription()
            + ")");
    System.out.println();

    Type type = Type.valueOf(insightName.split("\\.")[0].toUpperCase());

    for (Class<?> clazz : insight.getConfigurationClasses()) {
      showConfig(clazz, type);
    }
    return null;
  }

  protected void showConfig(Class<?> clazz, Type type) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.addResource(getHost(conf, new Component(type)) + "/conf");
    printConfig(clazz, conf);
  }

  /**
   * Print all the configuration annotated on the class or any superclass.
   */
  protected void printConfig(Class<?> clazz, OzoneConfiguration conf) {
    ConfigGroup configGroup = clazz.getAnnotation(ConfigGroup.class);
    if (configGroup == null) {
      return;
    }
    for (Field field : clazz.getDeclaredFields()) {
      if (field.isAnnotationPresent(Config.class)) {
        Config config = field.getAnnotation(Config.class);
        String key = config.key();
        System.out.println(">>> " + key);
        System.out.println("       default: " + config.defaultValue());
        System.out.println("       current: " + conf.get(key));
        System.out.println();
        System.out.println(config.description());
        System.out.println();
        System.out.println();
      }
    }
  }

}

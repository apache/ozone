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

package org.apache.hadoop.hdds.fs;

import java.io.File;
import java.time.Duration;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.conf.ConfigurationSource;

/**
 * Uses DU for all volumes excluding container data. Container data used space is from memory via container set.
 * Saves used value in cache file.
 */
public class DUOptimizedFactory implements SpaceUsageCheckFactory {

  private static final String DU_CACHE_FILE = "scmUsed";

  private DUFactory.Conf conf;

  @Override
  public SpaceUsageCheckFactory setConfiguration(
      ConfigurationSource configuration) {
    conf = configuration.getObject(DUFactory.Conf.class);
    return this;
  }

  @Override
  public SpaceUsageCheckParams paramsFor(File dir) {
    return null;
  }

  @Override
  public SpaceUsageCheckParams paramsFor(File dir, Supplier<File> exclusionProvider) {
    Duration refreshPeriod = conf.getRefreshPeriod();
    DUOptimized source = new DUOptimized(dir, exclusionProvider);
    SpaceUsagePersistence persistence = new SaveSpaceUsageToFile(
        new File(dir, DU_CACHE_FILE), refreshPeriod);

    SpaceUsageCheckParams params = new SpaceUsageCheckParams(dir, source, refreshPeriod, persistence);
    source.setContainerUsedSpaceProvider(params::getContainerUsedSpace);
    return params;
  }
}

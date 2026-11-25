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

package org.apache.hadoop.hdds.upgrade;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.ozone.upgrade.AbstractUpgradeActionProvider;
import org.apache.hadoop.ozone.upgrade.ScmUpgradeActionForLayoutFeature;
import org.apache.hadoop.ozone.upgrade.ScmUpgradeActionForVersion;

/**
 * Loads {@link ScmUpgradeAction} implementations annotated with {@link ScmUpgradeActionForLayoutFeature} or
 * {@link ScmUpgradeActionForVersion}.
 */
public final class ScmUpgradeActionProvider extends AbstractUpgradeActionProvider<ScmUpgradeAction> {

  public static final String SCM_UPGRADE_CLASS_PACKAGE = "org.apache.hadoop.hdds.scm.server";

  public ScmUpgradeActionProvider() {
    super(ImmutableSet.of(ScmUpgradeActionForLayoutFeature.class, ScmUpgradeActionForVersion.class),
        ScmUpgradeAction.class, SCM_UPGRADE_CLASS_PACKAGE);
  }

  @Override
  protected ComponentVersion extractVersion(Class<?> clazz) {
    ScmUpgradeActionForLayoutFeature layoutAnnotation = clazz.getAnnotation(ScmUpgradeActionForLayoutFeature.class);
    if (layoutAnnotation != null) {
      return layoutAnnotation.feature();
    }
    return clazz.getAnnotation(ScmUpgradeActionForVersion.class).version();
  }
}

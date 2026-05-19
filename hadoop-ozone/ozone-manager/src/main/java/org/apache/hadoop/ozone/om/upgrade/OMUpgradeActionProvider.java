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

package org.apache.hadoop.ozone.om.upgrade;

import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.ozone.upgrade.AbstractUpgradeActionProvider;

/**
 * Loads {@link OmUpgradeAction} implementations annotated with {@link UpgradeActionOm} from
 * {@link #OM_UPGRADE_CLASS_PACKAGE} only.
 */
public final class OMUpgradeActionProvider extends AbstractUpgradeActionProvider<OmUpgradeAction> {

  /**
   * Package scanned for {@link UpgradeActionOm}-annotated classes (production OM upgrade actions).
   */
  public static final String OM_UPGRADE_CLASS_PACKAGE = "org.apache.hadoop.ozone.om.upgrade";

  public OMUpgradeActionProvider() {
    super(UpgradeActionOm.class, OmUpgradeAction.class, OM_UPGRADE_CLASS_PACKAGE);
  }

  @Override
  protected ComponentVersion extractVersion(Class<?> clazz) {
    UpgradeActionOm annotation = clazz.getAnnotation(UpgradeActionOm.class);
    return annotation.feature();
  }
}

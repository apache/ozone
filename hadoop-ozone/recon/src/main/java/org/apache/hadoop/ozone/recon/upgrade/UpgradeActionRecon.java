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

package org.apache.hadoop.ozone.recon.upgrade;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The {@code UpgradeActionRecon} annotation is used to specify
 * upgrade actions that should be executed during finalization
 * of the Recon service layout upgrade process.
 *
 * <p>This annotation can be used to associate an upgrade action
 * class with a specific layout feature. The framework will
 * dynamically discover these annotated upgrade actions and execute
 * them based on the feature's version during finalization.
 *
 * <p>The annotation is retained at runtime, allowing the reflection-based
 * mechanism to scan for annotated classes, register the associated actions,
 * and execute them as necessary during the layout upgrade process.
 *
 * Example usage:
 *
 * <pre>
 * &#64;UpgradeActionRecon(feature = FEATURE_NAME)
 * public class FeatureNameUpgradeAction implements ReconUpgradeAction {
 *   &#64;Override
 *   public void execute(DataSource source) throws Exception {
 *     // Custom upgrade logic for FEATURE_NAME
 *   }
 * }
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface UpgradeActionRecon {

  /**
   * Defines the layout feature this upgrade action is associated with.
   */
  ReconLayoutFeature feature();
}

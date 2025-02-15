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

package org.apache.hadoop.ozone.upgrade;

/**
 * Read Only interface to an Ozone component's Version Manager.
 */
public interface LayoutVersionManager {

  /**
   * Get the Current Metadata Layout Version.
   * @return MLV
   */
  int getMetadataLayoutVersion();

  /**
   * Get the Current Software Layout Version.
   * @return SLV
   */
  int getSoftwareLayoutVersion();

  /**
   * Does it need finalization?
   * @return true/false
   */
  boolean needsFinalization();

  /**
   * Is allowed feature?
   * @param layoutFeature feature object
   * @return true/false.
   */
  boolean isAllowed(LayoutFeature layoutFeature);

  /**
   * Is allowed feature?
   * @param featureName feature name
   * @return true/false.
   */
  boolean isAllowed(String featureName);

  /**
   * Get Feature given feature name.
   * @param name Feature name.
   * @return LayoutFeature instance.
   */
  LayoutFeature getFeature(String name);

  /**
   * Get Feature given its layout version.
   * @param layoutVersion Version number of the feature.
   * @return LayoutFeature instance.
   */
  LayoutFeature getFeature(int layoutVersion);

  Iterable<? extends LayoutFeature> unfinalizedFeatures();

  /**
   * Generic API for returning a registered handler for a given type.
   * @param type String type
   */
  default Object getHandler(String type) {
    return null;
  }

  void close();

}

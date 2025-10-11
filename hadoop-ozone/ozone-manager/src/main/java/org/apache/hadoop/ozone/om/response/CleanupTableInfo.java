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

package org.apache.hadoop.ozone.om.response;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.hadoop.hdds.annotation.InterfaceStability;

/**
 * Annotation to provide information about clean up table information for
 * {@link OMClientResponse}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
@InterfaceStability.Evolving
public @interface CleanupTableInfo {

  /**
   * Array of tables affected by this operation. This information will be used
   * during cleanup table cache.
   * @return list of table names.
   */
  String[] cleanupTables() default {};

  /**
   * If all tables are affected, like at update finalization, one can specify
   * cleanupAll=true, instead of the list of all tables. In this case the
   * cleanupTable property has to be defined as an empty array (the default).
   * @return whether to cleanup all tables.
   */
  boolean cleanupAll() default false;
}

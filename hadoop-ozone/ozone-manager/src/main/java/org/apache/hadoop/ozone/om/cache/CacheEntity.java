/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.cache;

/**
 * Entities that are to be cached.
 */
public enum CacheEntity {

  DIR("directory");
  // This is extendable and one can add more entities for
  // caching based on demand. For example, define new entities like FILE
  // ("file"), LISTING("listing") cache etc.

  CacheEntity(String entity) {
    this.entityName = entity;
  }

  private String entityName;

  public String getName() {
    return entityName;
  }

  public static CacheEntity getEntity(String entityStr) {
    for (CacheEntity entity : CacheEntity.values()) {
      if (entity.getName().equalsIgnoreCase(entityStr)) {
        return entity;
      }
    }
    // no default
    return null;
  }
}

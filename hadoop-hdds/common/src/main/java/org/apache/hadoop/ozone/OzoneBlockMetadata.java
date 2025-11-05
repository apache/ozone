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

package org.apache.hadoop.ozone;

/**
 * Constants for block file metadata fields.
 * These metadata fields are stored with each block and provide
 * information about the key hierarchy and provenance.
 */
public final class OzoneBlockMetadata {

  /**
   * Metadata field indicating the type of entity (e.g., "KEY").
   */
  public static final String TYPE = "TYPE";

  /**
   * Metadata field for the volume name.
   */
  public static final String VOLUME_NAME = "VOLUME_NAME";

  /**
   * Metadata field for the bucket name.
   */
  public static final String BUCKET_NAME = "BUCKET_NAME";

  /**
   * Metadata field for the key name (full path including directories for FSO).
   */
  public static final String KEY_NAME = "KEY_NAME";

  /**
   * Metadata field for the object ID from Ozone Manager.
   */
  public static final String OBJECT_ID = "OBJECT_ID";

  /**
   * Metadata field for the parent object ID from Ozone Manager.
   */
  public static final String PARENT_OBJECT_ID = "PARENT_OBJECT_ID";

  /**
   * Metadata field for the block creation timestamp in ISO-8601 format.
   */
  public static final String CREATION_TIME = "CREATION_TIME";

  /**
   * Type value indicating a key entity.
   */
  public static final String TYPE_KEY = "KEY";

  private OzoneBlockMetadata() {
    // Prevent instantiation
  }
}

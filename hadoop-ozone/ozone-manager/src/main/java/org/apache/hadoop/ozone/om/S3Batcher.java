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

package org.apache.hadoop.ozone.om;

import java.io.IOException;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;

/**
 * Batcher for write and read operations. Depend on provide batch operator.
 */
public interface S3Batcher {
  /**
   * Add with provided batch.
   * @param batchOperator instance of batch operator.
   * @param id entity id.
   * @param s3SecretValue s3 secret value.
   * @throws IOException in case when batch operation failed.
   */
  void addWithBatch(AutoCloseable batchOperator, String id,
                    S3SecretValue s3SecretValue)
      throws IOException;

  /**
   * Delete with provided batch.
   * @param batchOperator instance of batch operator.
   * @param id entity id.
   * @throws IOException in case when batch operation failed.
   */
  void deleteWithBatch(AutoCloseable batchOperator, String id)
      throws IOException;
}

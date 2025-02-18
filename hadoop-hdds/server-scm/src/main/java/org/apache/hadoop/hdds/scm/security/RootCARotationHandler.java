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

package org.apache.hadoop.hdds.scm.security;

import java.io.IOException;
import org.apache.hadoop.hdds.scm.metadata.Replicate;

/**
 * This interface defines APIs for sub-ca rotation instructions.
 */
public interface RootCARotationHandler {

  /**
   * Notify SCM peers to do sub-ca rotation preparation and replicate
   * this operation through RATIS.
   * @param rootCertId the new root certificate serial ID
   * @throws IOException on failure to persist configuration
   */
  @Replicate
  void rotationPrepare(String rootCertId)
      throws IOException;

  @Replicate(invocationType = Replicate.InvocationType.CLIENT)
  void rotationPrepareAck(String rootCertId, String scmCertId, String scmId)
      throws IOException;

  @Replicate
  void rotationCommit(String rootCertId)
      throws IOException;

  @Replicate
  void rotationCommitted(String rootCertId)
      throws IOException;

  int rotationPrepareAcks();

  void resetRotationPrepareAcks();

  void setSubCACertId(String subCACertId);
}

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

package org.apache.hadoop.ozone.security.acl;

import java.util.Objects;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.om.exceptions.OMException;

/**
 * SharedTmp implementation of {@link IAccessAuthorizer}.
 */
public class SharedTmpDirAuthorizer implements IAccessAuthorizer {

  private final OzoneNativeAuthorizer ozoneNativeAuthorizer;
  private final IAccessAuthorizer authorizer;

  public SharedTmpDirAuthorizer(OzoneNativeAuthorizer ozoneNativeAuthorizer,
      IAccessAuthorizer authorizer) {
    this.ozoneNativeAuthorizer = ozoneNativeAuthorizer;
    this.authorizer = authorizer;
  }

  /**
   * Check access for a given ozoneObject.
   *
   * @param ozObject object for which access needs to be checked.
   * @param context Context object encapsulating all user related information.
   * @return true if user has access else false.
   */
  @Override
  public boolean checkAccess(IOzoneObj ozObject, RequestContext context)
      throws OMException {
    Objects.requireNonNull(ozObject);
    Objects.requireNonNull(context);

    if (ozObject instanceof OzoneObjInfo) {
      OzoneObjInfo objInfo = (OzoneObjInfo) ozObject;
      if (OFSPath.isSharedTmpBucket(objInfo)) {
        return ozoneNativeAuthorizer.checkAccess(ozObject, context);
      }
    }
    return authorizer.checkAccess(ozObject, context);
  }
}

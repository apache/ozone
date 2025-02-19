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

package org.apache.hadoop.ozone.shell.acl;

import java.io.IOException;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.StoreTypeOption;
import picocli.CommandLine;

/**
 * Base class for ACL-related commands.
 */
public abstract class AclHandler extends Handler {

  public static final String ADD_ACL_NAME = "addacl";
  public static final String ADD_ACL_DESC = "Add one or more new ACLs.";

  public static final String GET_ACL_NAME = "getacl";
  public static final String GET_ACL_DESC = "List all ACLs.";

  public static final String REMOVE_ACL_NAME = "removeacl";
  public static final String REMOVE_ACL_DESC = "Remove one or more existing " +
      "ACLs.";

  public static final String SET_ACL_NAME = "setacl";
  public static final String SET_ACL_DESC = "Set one or more ACLs, replacing " +
      "the existing ones.";

  @CommandLine.Mixin
  private StoreTypeOption storeType;

  protected abstract void execute(OzoneClient client, OzoneObj obj)
      throws IOException;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    execute(client, address.toOzoneObj(storeType.getValue()));
  }

}

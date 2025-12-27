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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import picocli.CommandLine.Option;

/**
 * Get ACLs.
 */
public abstract class GetAclHandler extends AclHandler {

  @Option(names = {"--string", "-o"},
      description = "Output ACLs as a comma-separated string in the format " +
          "that can be used with setacl/addacl commands")
  private boolean stringFormat;

  @Override
  protected void execute(OzoneClient client, OzoneObj obj) throws IOException {
    List<OzoneAcl> result = client.getObjectStore().getAcl(obj);
    if (stringFormat) {
      printAclsAsString(result);
    } else {
      printObjectAsJson(result);
    }
  }

  /**
   * Prints ACLs as a comma-separated string in the format:
   * type:name:permissions or type:name:permissions[scope] if scope is DEFAULT.
   * This format is compatible with setacl/addacl commands.
   *
   * @param acls List of OzoneAcl objects to print
   */
  private void printAclsAsString(List<OzoneAcl> acls) {
    String aclString = acls.stream()
        .map(this::formatAcl)
        .collect(Collectors.joining(","));
    out().println(aclString);
  }

  /**
   * Formats a single ACL for string output.
   * Omits the scope if it's ACCESS (the default) to match the input format.
   *
   * @param acl The OzoneAcl to format
   * @return Formatted ACL string
   */
  private String formatAcl(OzoneAcl acl) {
    String baseAcl = acl.toString();
    // If the scope is ACCESS (default), remove it from the output
    if (acl.getAclScope() == OzoneAcl.AclScope.ACCESS) {
      // Remove [ACCESS] from the end
      return baseAcl.replaceAll("\\[ACCESS\\]$", "");
    }
    return baseAcl;
  }

}

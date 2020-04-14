/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.shell.acl;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

/**
 * Defines command-line option for specifying one or more ACLs.
 */
public class AclOption implements CommandLine.ITypeConverter<OzoneAcl> {

  @CommandLine.Option(names = {"--acls", "--acl", "-al", "-a"}, split = ",",
      required = true,
      converter = AclOption.class,
      description = "Comma separated ACL list:\n" +
          "Example: user:user2:a OR user:user1:rw,group:hadoop:a\n" +
          "r = READ, " +
          "w = WRITE, " +
          "c = CREATE, " +
          "d = DELETE, " +
          "l = LIST, " +
          "a = ALL, " +
          "n = NONE, " +
          "x = READ_ACL, " +
          "y = WRITE_ACL.")
  private OzoneAcl[] values;

  private List<OzoneAcl> getAclList() {
    return ImmutableList.copyOf(values);
  }

  public void addTo(OzoneObj obj, ObjectStore objectStore, PrintStream out)
      throws IOException {
    for (OzoneAcl acl : getAclList()) {
      boolean result = objectStore.addAcl(obj, acl);

      String message = result
          ? ("ACL %s added successfully.%n")
          : ("ACL %s already exists.%n");

      out.printf(message, acl);
    }
  }

  public void removeFrom(OzoneObj obj, ObjectStore objectStore, PrintStream out)
      throws IOException {
    for (OzoneAcl acl : getAclList()) {
      boolean result = objectStore.removeAcl(obj, acl);

      String message = result
          ? ("ACL %s removed successfully.%n")
          : ("ACL %s doesn't exist.%n");

      out.printf(message, acl);
    }
  }

  public void setOn(OzoneObj obj, ObjectStore objectStore, PrintStream out)
      throws IOException {
    objectStore.setAcl(obj, getAclList());
    out.println("ACLs set successfully.");
  }

  @Override
  public OzoneAcl convert(String value) {
    return OzoneAcl.parseAcl(value);
  }
}

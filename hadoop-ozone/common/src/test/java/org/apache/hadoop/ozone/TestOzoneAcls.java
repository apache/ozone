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

import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.CREATE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.DELETE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.LIST;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.NONE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ_ACL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE_ACL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.junit.jupiter.api.Test;

/**
 * This class is to test acl storage and retrieval in ozone store.
 */
class TestOzoneAcls {

  @Test
  void testAclParse() {
    HashMap<String, Boolean> testMatrix;
    testMatrix = new HashMap<>();

    testMatrix.put("user:bilbo:r", Boolean.TRUE);
    testMatrix.put("user:bilbo:w", Boolean.TRUE);
    testMatrix.put("user:bilbo:rw", Boolean.TRUE);
    testMatrix.put("user:bilbo:a", Boolean.TRUE);
    testMatrix.put("    user:bilbo:a   ", Boolean.TRUE);


    // ACLs makes no judgement on the quality of
    // user names. it is for the userAuth interface
    // to determine if a user name is really a name
    testMatrix.put(" user:*:rw", Boolean.TRUE);
    testMatrix.put(" user:~!:rw", Boolean.TRUE);


    testMatrix.put("", Boolean.FALSE);
    testMatrix.put(null, Boolean.FALSE);
    testMatrix.put(" user:bilbo:", Boolean.FALSE);
    testMatrix.put(" user:bilbo:rx", Boolean.TRUE);
    testMatrix.put(" user:bilbo:rwdlncxy", Boolean.TRUE);
    testMatrix.put(" group:bilbo:rwdlncxy", Boolean.TRUE);
    testMatrix.put(" world::rwdlncxy", Boolean.TRUE);
    testMatrix.put(" user:bilbo:rncxy", Boolean.TRUE);
    testMatrix.put(" group:bilbo:ncxy", Boolean.TRUE);
    testMatrix.put(" world::ncxy", Boolean.TRUE);
    testMatrix.put(" user:bilbo:rwcxy", Boolean.TRUE);
    testMatrix.put(" group:bilbo:rwcxy", Boolean.TRUE);
    testMatrix.put(" world::rwcxy", Boolean.TRUE);
    testMatrix.put(" user:bilbo:mk", Boolean.FALSE);
    testMatrix.put(" user::rw", Boolean.FALSE);
    testMatrix.put("user11:bilbo:rw", Boolean.FALSE);
    testMatrix.put(" user:::rw", Boolean.FALSE);

    testMatrix.put(" group:hobbit:r", Boolean.TRUE);
    testMatrix.put(" group:hobbit:w", Boolean.TRUE);
    testMatrix.put(" group:hobbit:rw", Boolean.TRUE);
    testMatrix.put(" group:hobbit:a", Boolean.TRUE);
    testMatrix.put(" group:*:rw", Boolean.TRUE);
    testMatrix.put(" group:~!:rw", Boolean.TRUE);

    testMatrix.put(" group:hobbit:", Boolean.FALSE);
    testMatrix.put(" group:hobbit:rx", Boolean.TRUE);
    testMatrix.put(" group:hobbit:mk", Boolean.FALSE);
    testMatrix.put(" group::", Boolean.FALSE);
    testMatrix.put(" group::rw", Boolean.FALSE);
    testMatrix.put(" group22:hobbit:", Boolean.FALSE);
    testMatrix.put(" group:::rw", Boolean.FALSE);

    testMatrix.put("JUNK group:hobbit:r", Boolean.FALSE);
    testMatrix.put("JUNK group:hobbit:w", Boolean.FALSE);
    testMatrix.put("JUNK group:hobbit:rw", Boolean.FALSE);
    testMatrix.put("JUNK group:hobbit:a", Boolean.FALSE);
    testMatrix.put("JUNK group:*:rw", Boolean.FALSE);
    testMatrix.put("JUNK group:~!:rw", Boolean.FALSE);

    testMatrix.put(" world::r", Boolean.TRUE);
    testMatrix.put(" world::w", Boolean.TRUE);
    testMatrix.put(" world::rw", Boolean.TRUE);
    testMatrix.put(" world::a", Boolean.TRUE);

    testMatrix.put(" world:bilbo:w", Boolean.FALSE);
    testMatrix.put(" world:bilbo:rw", Boolean.FALSE);
    testMatrix.put(" anonymous:bilbo:w", Boolean.FALSE);
    testMatrix.put(" anonymous:ANONYMOUS:w", Boolean.TRUE);
    testMatrix.put(" anonymous::rw", Boolean.TRUE);
    testMatrix.put(" world:WORLD:rw", Boolean.TRUE);

    for (Map.Entry<String, Boolean> entry : testMatrix.entrySet()) {
      if (entry.getValue()) {
        OzoneAcl.parseAcl(entry.getKey());
      } else {
        assertThrows(IllegalArgumentException.class, () -> OzoneAcl.parseAcl(entry.getKey()));
      }
    }
  }

  @Test
  void testAclValues() {
    OzoneAcl acl = OzoneAcl.parseAcl("user:bilbo:rw");
    assertEquals(acl.getName(), "bilbo");
    assertTrue(acl.isSet(READ));
    assertTrue(acl.isSet(WRITE));
    assertFalse(acl.isSet(ALL));
    assertFalse(acl.isSet(READ_ACL));
    assertEquals(ACLIdentityType.USER, acl.getType());

    acl = OzoneAcl.parseAcl("user:bilbo:a");
    assertEquals("bilbo", acl.getName());
    assertTrue(acl.isSet(ALL));
    assertFalse(acl.isSet(WRITE));
    assertEquals(ACLIdentityType.USER, acl.getType());

    acl = OzoneAcl.parseAcl("user:bilbo:r");
    assertEquals("bilbo", acl.getName());
    assertTrue(acl.isSet(READ));
    assertFalse(acl.isSet(ALL));
    assertEquals(ACLIdentityType.USER, acl.getType());

    acl = OzoneAcl.parseAcl("user:bilbo:w");
    assertEquals("bilbo", acl.getName());
    assertTrue(acl.isSet(WRITE));
    assertFalse(acl.isSet(ALL));
    assertEquals(ACLIdentityType.USER, acl.getType());

    acl = OzoneAcl.parseAcl("group:hobbit:a");
    assertEquals(acl.getName(), "hobbit");
    assertTrue(acl.isSet(ALL));
    assertFalse(acl.isSet(READ));
    assertEquals(ACLIdentityType.GROUP, acl.getType());

    acl = OzoneAcl.parseAcl("world::a");
    assertEquals(acl.getName(), "WORLD");
    assertTrue(acl.isSet(ALL));
    assertFalse(acl.isSet(WRITE));
    assertEquals(ACLIdentityType.WORLD, acl.getType());

    acl = OzoneAcl.parseAcl("user:bilbo:rwdlncxy");
    assertEquals(acl.getName(), "bilbo");
    assertTrue(acl.isSet(READ));
    assertTrue(acl.isSet(WRITE));
    assertTrue(acl.isSet(DELETE));
    assertTrue(acl.isSet(LIST));
    assertTrue(acl.isSet(NONE));
    assertTrue(acl.isSet(CREATE));
    assertTrue(acl.isSet(READ_ACL));
    assertTrue(acl.isSet(WRITE_ACL));
    assertFalse(acl.isSet(ALL));

    acl = OzoneAcl.parseAcl("group:hadoop:rwdlncxy");
    assertEquals(acl.getName(), "hadoop");
    assertTrue(acl.isSet(READ));
    assertTrue(acl.isSet(WRITE));
    assertTrue(acl.isSet(DELETE));
    assertTrue(acl.isSet(LIST));
    assertTrue(acl.isSet(NONE));
    assertTrue(acl.isSet(CREATE));
    assertTrue(acl.isSet(READ_ACL));
    assertTrue(acl.isSet(WRITE_ACL));
    assertFalse(acl.isSet(ALL));
    assertEquals(ACLIdentityType.GROUP, acl.getType());

    acl = OzoneAcl.parseAcl("world::rwdlncxy");
    assertEquals(acl.getName(), "WORLD");
    assertTrue(acl.isSet(READ));
    assertTrue(acl.isSet(WRITE));
    assertTrue(acl.isSet(DELETE));
    assertTrue(acl.isSet(LIST));
    assertTrue(acl.isSet(NONE));
    assertTrue(acl.isSet(CREATE));
    assertTrue(acl.isSet(READ_ACL));
    assertTrue(acl.isSet(WRITE_ACL));
    assertFalse(acl.isSet(ALL));
    assertEquals(ACLIdentityType.WORLD, acl.getType());

    // Acls with scope info.
    acl = OzoneAcl.parseAcl("user:bilbo:rwdlncxy[DEFAULT]");
    assertEquals(acl.getName(), "bilbo");
    assertTrue(acl.isSet(READ));
    assertTrue(acl.isSet(WRITE));
    assertTrue(acl.isSet(DELETE));
    assertTrue(acl.isSet(LIST));
    assertTrue(acl.isSet(NONE));
    assertTrue(acl.isSet(CREATE));
    assertTrue(acl.isSet(READ_ACL));
    assertTrue(acl.isSet(WRITE_ACL));
    assertFalse(acl.isSet(ALL));
    assertEquals(acl.getAclScope(), OzoneAcl.AclScope.DEFAULT);

    acl = OzoneAcl.parseAcl("user:bilbo:rwdlncxy[ACCESS]");
    assertEquals(acl.getName(), "bilbo");
    assertTrue(acl.isSet(READ));
    assertTrue(acl.isSet(WRITE));
    assertTrue(acl.isSet(DELETE));
    assertTrue(acl.isSet(LIST));
    assertTrue(acl.isSet(NONE));
    assertTrue(acl.isSet(CREATE));
    assertTrue(acl.isSet(READ_ACL));
    assertTrue(acl.isSet(WRITE_ACL));
    assertFalse(acl.isSet(ALL));
    assertEquals(acl.getAclScope(), OzoneAcl.AclScope.ACCESS);

    acl = OzoneAcl.parseAcl("group:hadoop:rwdlncxy[ACCESS]");
    assertEquals(acl.getName(), "hadoop");
    assertTrue(acl.isSet(READ));
    assertTrue(acl.isSet(WRITE));
    assertTrue(acl.isSet(DELETE));
    assertTrue(acl.isSet(LIST));
    assertTrue(acl.isSet(NONE));
    assertTrue(acl.isSet(CREATE));
    assertTrue(acl.isSet(READ_ACL));
    assertTrue(acl.isSet(WRITE_ACL));
    assertFalse(acl.isSet(ALL));
    assertEquals(ACLIdentityType.GROUP, acl.getType());
    assertEquals(acl.getAclScope(), OzoneAcl.AclScope.ACCESS);

    acl = OzoneAcl.parseAcl("world::rwdlncxy[DEFAULT]");
    assertEquals(acl.getName(), "WORLD");
    assertTrue(acl.isSet(READ));
    assertTrue(acl.isSet(WRITE));
    assertTrue(acl.isSet(DELETE));
    assertTrue(acl.isSet(LIST));
    assertTrue(acl.isSet(NONE));
    assertTrue(acl.isSet(CREATE));
    assertTrue(acl.isSet(READ_ACL));
    assertTrue(acl.isSet(WRITE_ACL));
    assertFalse(acl.isSet(ALL));
    assertEquals(ACLIdentityType.WORLD, acl.getType());
    assertEquals(OzoneAcl.AclScope.DEFAULT, acl.getAclScope());

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> OzoneAcl.parseAcl("world::rwdlncxncxdfsfgbny"));
    assertThat(exception).hasMessageContaining("ACL right is not");
  }

  @Test
  void testBitSetToListConversion() {
    OzoneAcl acl = OzoneAcl.parseAcl("user:bilbo:rw");

    List<ACLType> rights = acl.getAclList();
    assertEquals(2, rights.size());
    assertTrue(rights.contains(READ));
    assertTrue(rights.contains(WRITE));
    assertFalse(rights.contains(CREATE));

    acl = OzoneAcl.parseAcl("user:bilbo:a");

    rights = acl.getAclList();
    assertEquals(1, rights.size());
    assertTrue(rights.contains(ALL));
    assertFalse(rights.contains(WRITE));
    assertFalse(rights.contains(CREATE));

    acl = OzoneAcl.parseAcl("user:bilbo:cxy");
    rights = acl.getAclList();
    assertEquals(3, rights.size());
    assertTrue(rights.contains(CREATE));
    assertTrue(rights.contains(READ_ACL));
    assertTrue(rights.contains(WRITE_ACL));
    assertFalse(rights.contains(WRITE));
    assertFalse(rights.contains(READ));

    List<OzoneAcl> acls = OzoneAcl.parseAcls("user:bilbo:cxy,group:hadoop:a");
    assertEquals(2, acls.size());
    rights = acls.get(0).getAclList();
    assertEquals(3, rights.size());
    assertTrue(rights.contains(CREATE));
    assertTrue(rights.contains(READ_ACL));
    assertTrue(rights.contains(WRITE_ACL));
    assertFalse(rights.contains(WRITE));
    assertFalse(rights.contains(READ));
    rights = acls.get(1).getAclList();
    assertTrue(rights.contains(ALL));

    acls = OzoneAcl.parseAcls("user:bilbo:cxy[ACCESS]," +
        "group:hadoop:a[DEFAULT],world::r[DEFAULT]");
    assertEquals(3, acls.size());
    rights = acls.get(0).getAclList();
    assertEquals(3, rights.size());
    assertTrue(rights.contains(CREATE));
    assertTrue(rights.contains(READ_ACL));
    assertTrue(rights.contains(WRITE_ACL));
    assertFalse(rights.contains(WRITE));
    assertFalse(rights.contains(READ));
    rights = acls.get(1).getAclList();
    assertTrue(rights.contains(ALL));

    assertEquals("bilbo", acls.get(0).getName());
    assertEquals("hadoop", acls.get(1).getName());
    assertEquals("WORLD", acls.get(2).getName());
    assertEquals(OzoneAcl.AclScope.ACCESS, acls.get(0).getAclScope());
    assertEquals(OzoneAcl.AclScope.DEFAULT, acls.get(1).getAclScope());
    assertEquals(OzoneAcl.AclScope.DEFAULT, acls.get(2).getAclScope());
  }
}

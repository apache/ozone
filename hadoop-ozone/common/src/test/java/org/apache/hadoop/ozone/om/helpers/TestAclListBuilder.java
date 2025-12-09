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

package org.apache.hadoop.ozone.om.helpers;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.hadoop.ozone.OzoneAcl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestAclListBuilder {

  private static final OzoneAcl ALICE_READ = OzoneAcl.of(USER, "alice", ACCESS, READ);
  private static final OzoneAcl ALICE_WRITE = OzoneAcl.of(USER, "alice", ACCESS, WRITE);
  private static final OzoneAcl ALICE_READ_WRITE = ALICE_READ.add(ALICE_WRITE);
  private static final OzoneAcl BOB_READ = OzoneAcl.of(USER, "bob", ACCESS, READ);

  public static Stream<List<OzoneAcl>> initialLists() {
    return Stream.of(
        emptyList(),
        singletonList(ALICE_READ),
        asList(ALICE_READ, ALICE_WRITE)
    );
  }

  @Test
  void returnsInitialListIfUnchanged() {
    ImmutableList<OzoneAcl> list = ImmutableList.copyOf(asList(ALICE_READ, ALICE_WRITE));
    AclListBuilder subject = AclListBuilder.of(list);

    assertThat(subject.build())
        .isSameAs(list);
  }

  @Test
  void testAdd() {
    testAdd(subject -> {
      subject.add(ALICE_WRITE);
      subject.add(BOB_READ);
    });
  }

  @Test
  void testAddAll() {
    testAdd(subject -> subject.addAll(asList(ALICE_WRITE, BOB_READ)));
  }

  // op should be adding ALICE_WRITE and BOB_READ
  private void testAdd(Consumer<AclListBuilder> op) {
    AclListBuilder subject = AclListBuilder.copyOf(singletonList(ALICE_READ));

    op.accept(subject);

    assertThat(subject.isChanged())
        .isTrue();
    assertThat(subject.build())
        .contains(ALICE_READ_WRITE)
        .contains(BOB_READ)
        .hasSize(2);
  }

  @ParameterizedTest
  @MethodSource("initialLists")
  void testSetSame(List<OzoneAcl> initialList) {
    AclListBuilder subject = AclListBuilder.copyOf(initialList);

    subject.set(initialList);

    assertThat(subject.isChanged())
        .isFalse();
    assertThat(subject.build())
        .isEqualTo(initialList);
  }

  @ParameterizedTest
  @MethodSource("initialLists")
  void testSetEqual(List<OzoneAcl> initialList) {
    AclListBuilder subject = AclListBuilder.copyOf(initialList);

    subject.set(new ArrayList<>(initialList));

    assertThat(subject.isChanged())
        .isFalse();
    assertThat(subject.build())
        .isEqualTo(initialList);
  }

  @ParameterizedTest
  @MethodSource("initialLists")
  void testSetDifferent(List<OzoneAcl> initialList) {
    AclListBuilder subject = AclListBuilder.copyOf(initialList);
    List<OzoneAcl> differentList = new ArrayList<>(initialList);
    differentList.add(BOB_READ);

    subject.set(differentList);

    assertThat(subject.isChanged())
        .isTrue();
    assertThat(subject.build())
        .isEqualTo(differentList);
  }

  @Test
  void testRemove() {
    AclListBuilder subject = AclListBuilder.copyOf(asList(ALICE_READ_WRITE, BOB_READ));

    subject.remove(ALICE_WRITE);
    subject.remove(BOB_READ);

    assertThat(subject.isChanged())
        .isTrue();
    assertThat(subject.build())
        .isEqualTo(singletonList(ALICE_READ));
  }
}

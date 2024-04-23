/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package org.apache.hadoop.ozone;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclScope;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.NONE;

/**
 * OzoneACL classes define bucket ACLs used in OZONE.
 *
 * ACLs in Ozone follow this pattern.
 * <ul>
 * <li>user:name:rw
 * <li>group:name:rw
 * <li>world::rw
 * </ul>
 */
@Immutable
public class OzoneAcl {

  private static final String ACL_SCOPE_REGEX = ".*\\[(ACCESS|DEFAULT)\\]";
  private final ACLIdentityType type;
  private final String name;
  @JsonIgnore
  private final BitSet aclBitSet;
  private final AclScope aclScope;
  private static final List<ACLType> EMPTY_LIST = new ArrayList<>(0);

  public OzoneAcl(ACLIdentityType type, String name, AclScope scope, ACLType... acls) {
    this(type, name, scope, bitSetOf(acls));
  }

  public OzoneAcl(ACLIdentityType type, String name, AclScope scope, EnumSet<ACLType> acls) {
    this(type, name, scope, bitSetOf(acls.toArray(new ACLType[0])));
  }

  private OzoneAcl(ACLIdentityType type, String name, AclScope scope, BitSet acls) {
    this.name = validateNameAndType(type, name);
    this.type = type;
    this.aclScope = scope;
    this.aclBitSet = acls;
  }

  private static BitSet bitSetOf(ACLType... acls) {
    BitSet bits = new BitSet();
    if (acls != null && acls.length > 0) {
      for (ACLType acl : acls) {
        bits.set(acl.ordinal());
      }
    }
    return bits;
  }

  private static BitSet validateAndCopy(BitSet acls) {
    Objects.requireNonNull(acls);

    if (acls.cardinality() > ACLType.getNoOfAcls()) {
      throw new IllegalArgumentException("Acl bitset passed has unexpected " +
          "size. bitset size:" + acls.cardinality() + ", bitset:" + acls);
    }

    return copyBitSet(acls);
  }

  private static BitSet copyBitSet(BitSet acls) {
    return (BitSet) acls.clone();
  }

  private static String validateNameAndType(ACLIdentityType type, String name) {
    Objects.requireNonNull(type);

    if (type == ACLIdentityType.WORLD || type == ACLIdentityType.ANONYMOUS) {
      if (!name.equals(ACLIdentityType.WORLD.name()) &&
          !name.equals(ACLIdentityType.ANONYMOUS.name()) &&
          name.length() != 0) {
        throw new IllegalArgumentException("Expected name " + type.name() + ", but was: " + name);
      }
      // For type WORLD and ANONYMOUS we allow only one acl to be set.
      return type.name();
    }

    if (((type == ACLIdentityType.USER) || (type == ACLIdentityType.GROUP))
        && (name.length() == 0)) {
      throw new IllegalArgumentException(type + " name is required");
    }

    return name;
  }

  public OzoneAcl withScope(final AclScope scope) {
    return scope == aclScope ? this
        : new OzoneAcl(type, name, scope, copyBitSet(aclBitSet));
  }

  /**
   * Parses an ACL string and returns the ACL object. If acl scope is not
   * passed in input string then scope is set to ACCESS.
   *
   * @param acl - Acl String , Ex. user:anu:rw
   *
   * @return - Ozone ACLs
   */
  public static OzoneAcl parseAcl(String acl)
      throws IllegalArgumentException {
    if ((acl == null) || acl.isEmpty()) {
      throw new IllegalArgumentException("ACLs cannot be null or empty");
    }
    String[] parts = acl.trim().split(":");
    if (parts.length < 3) {
      throw new IllegalArgumentException("ACLs are not in expected format");
    }

    ACLIdentityType aclType = ACLIdentityType.valueOf(parts[0].toUpperCase());

    String bits = parts[2];

    // Default acl scope is ACCESS.
    AclScope aclScope = AclScope.ACCESS;

    // Check if acl string contains scope info.
    if (parts[2].matches(ACL_SCOPE_REGEX)) {
      int indexOfOpenBracket = parts[2].indexOf("[");
      bits = parts[2].substring(0, indexOfOpenBracket);
      aclScope = AclScope.valueOf(parts[2].substring(indexOfOpenBracket + 1,
          parts[2].indexOf("]")));
    }

    EnumSet<ACLType> acls = EnumSet.noneOf(ACLType.class);
    for (char ch : bits.toCharArray()) {
      acls.add(ACLType.getACLRight(String.valueOf(ch)));
    }

    // TODO : Support sanitation of these user names by calling into
    // userAuth Interface.
    return new OzoneAcl(aclType, parts[1], aclScope, acls);
  }

  /**
   * Parses an ACL string and returns the ACL object.
   *
   * @param acls - Acl String , Ex. user:anu:rw
   *
   * @return - Ozone ACLs
   */
  public static List<OzoneAcl> parseAcls(String acls)
      throws IllegalArgumentException {
    if ((acls == null) || acls.isEmpty()) {
      throw new IllegalArgumentException("ACLs cannot be null or empty");
    }
    String[] parts = acls.trim().split(",");
    if (parts.length < 1) {
      throw new IllegalArgumentException("ACLs are not in expected format");
    }
    List<OzoneAcl> ozAcls = new ArrayList<>();

    for (String acl:parts) {
      ozAcls.add(parseAcl(acl));
    }
    return ozAcls;
  }

  public static OzoneAclInfo toProtobuf(OzoneAcl acl) {
    OzoneAclInfo.Builder builder = OzoneAclInfo.newBuilder()
        .setName(acl.getName())
        .setType(OzoneAclType.valueOf(acl.getType().name()))
        .setAclScope(OzoneAclScope.valueOf(acl.getAclScope().name()))
        .setRights(ByteString.copyFrom(acl.getAclByteArray()));
    return builder.build();
  }

  public static OzoneAcl fromProtobuf(OzoneAclInfo protoAcl) {
    BitSet aclRights = BitSet.valueOf(protoAcl.getRights().toByteArray());
    return new OzoneAcl(ACLIdentityType.valueOf(protoAcl.getType().name()), protoAcl.getName(),
        AclScope.valueOf(protoAcl.getAclScope().name()), validateAndCopy(aclRights));
  }

  public AclScope getAclScope() {
    return aclScope;
  }

  @Override
  public String toString() {
    return type + ":" + name + ":" + ACLType.getACLString(aclBitSet)
        + "[" + aclScope + "]";
  }

  /**
   * Returns a hash code value for the object. This method is
   * supported for the benefit of hash tables.
   *
   * @return a hash code value for this object.
   *
   * @see Object#equals(Object)
   * @see System#identityHashCode
   */
  @Override
  public int hashCode() {
    return Objects.hash(this.getName(), aclBitSet,
                        this.getType().toString(), this.getAclScope());
  }

  /**
   * Returns name.
   *
   * @return name
   */
  public String getName() {
    return name;
  }

  @JsonIgnore
  public boolean isEmpty() {
    return aclBitSet.isEmpty();
  }

  @VisibleForTesting
  public boolean isSet(ACLType acl) {
    return aclBitSet.get(acl.ordinal());
  }

  public boolean checkAccess(ACLType acl) {
    return (isSet(acl) || isSet(ALL)) && !isSet(NONE);
  }

  public OzoneAcl add(OzoneAcl other) {
    return apply(bits -> bits.or(other.aclBitSet));
  }

  public OzoneAcl remove(OzoneAcl other) {
    return apply(bits -> bits.andNot(other.aclBitSet));
  }

  /** @return copy of this {@code OzoneAcl} after applying the given {@code op},
   * or this instance if {@code op} makes no difference */
  private OzoneAcl apply(Consumer<BitSet> op) {
    final BitSet cloneBits = copyBitSet(aclBitSet);
    op.accept(cloneBits);
    return cloneBits.equals(aclBitSet)
        ? this
        : new OzoneAcl(type, name, aclScope, cloneBits);
  }

  @JsonIgnore
  public byte[] getAclByteArray() {
    return aclBitSet.toByteArray();
  }

  public List<ACLType> getAclList() {
    if (aclBitSet != null) {
      return aclBitSet.stream().mapToObj(a ->
          ACLType.values()[a]).collect(Collectors.toList());
    }
    return EMPTY_LIST;
  }

  /**
   * Returns Type.
   *
   * @return type
   */
  public ACLIdentityType getType() {
    return type;
  }

  /**
   * Indicates whether some other object is "equal to" this one.
   *
   * @param obj the reference object with which to compare.
   *
   * @return {@code true} if this object is the same as the obj
   * argument; {@code false} otherwise.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    OzoneAcl otherAcl = (OzoneAcl) obj;
    return otherAcl.getName().equals(this.getName()) &&
        otherAcl.getType().equals(this.getType()) &&
        Objects.equals(aclBitSet, otherAcl.aclBitSet) &&
        otherAcl.getAclScope().equals(this.getAclScope());
  }

  /**
   * Scope of ozone acl.
   * */
  public enum AclScope {
    ACCESS,
    DEFAULT;
  }
}

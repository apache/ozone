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

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.NONE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.ProtoUtils;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclScope;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.ratis.util.MemoizedSupplier;

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
public final class OzoneAcl {

  private static final String ACL_SCOPE_REGEX = ".*\\[(ACCESS|DEFAULT)\\]";
  /**
   * Link bucket default acl defined [world::rw]
   * which is similar to Linux POSIX symbolic.
   */
  public static final OzoneAcl LINK_BUCKET_DEFAULT_ACL =
      OzoneAcl.of(IAccessAuthorizer.ACLIdentityType.WORLD, "", ACCESS, READ, WRITE);

  private final ACLIdentityType type;
  private final String name;
  @JsonIgnore
  private final int aclBits;
  private final AclScope aclScope;

  @JsonIgnore
  private final Supplier<String> toStringMethod;
  @JsonIgnore
  private final Supplier<Integer> hashCodeMethod;

  public static OzoneAcl of(ACLIdentityType type, String name, AclScope scope, ACLType... acls) {
    return new OzoneAcl(type, name, scope, toInt(acls));
  }

  public static OzoneAcl of(ACLIdentityType type, String name, AclScope scope, Set<ACLType> acls) {
    return new OzoneAcl(type, name, scope, toInt(acls));
  }

  private OzoneAcl(ACLIdentityType type, String name, AclScope scope, int acls) {
    this.name = validateNameAndType(type, name);
    this.type = type;
    this.aclScope = scope;
    this.aclBits = acls;

    this.toStringMethod = MemoizedSupplier.valueOf(() -> getType() + ":" + getName() + ":"
        + ACLType.getACLString(BitSet.valueOf(getAclByteString().asReadOnlyByteBuffer())) + "[" + getAclScope() + "]");
    this.hashCodeMethod = MemoizedSupplier.valueOf(() -> Objects.hash(getName(),
        BitSet.valueOf(getAclByteString().asReadOnlyByteBuffer()), getType().toString(), getAclScope()));
  }

  private static int toInt(int aclTypeOrdinal) {
    return 1 << aclTypeOrdinal;
  }

  private static int toInt(ACLType acl) {
    return toInt(acl.ordinal());
  }

  private static int toInt(ACLType[] acls) {
    if (acls == null) {
      return 0;
    }
    int value = 0;
    for (ACLType acl : acls) {
      value |= toInt(acl);
    }
    return value;
  }

  private static int toInt(Iterable<ACLType> acls) {
    if (acls == null) {
      return 0;
    }
    int value = 0;
    for (ACLType acl : acls) {
      value |= toInt(acl);
    }
    return value;
  }

  private static String validateNameAndType(ACLIdentityType type, String name) {
    Objects.requireNonNull(type);

    if (type == ACLIdentityType.WORLD || type == ACLIdentityType.ANONYMOUS) {
      if (!name.equals(ACLIdentityType.WORLD.name()) &&
          !name.equals(ACLIdentityType.ANONYMOUS.name()) &&
          !name.isEmpty()) {
        throw new IllegalArgumentException("Expected name " + type.name() + ", but was: " + name);
      }
      // For type WORLD and ANONYMOUS we allow only one acl to be set.
      return type.name();
    }

    if (((type == ACLIdentityType.USER) || (type == ACLIdentityType.GROUP))
        && (name.isEmpty())) {
      throw new IllegalArgumentException(type + " name is required");
    }

    return name;
  }

  public OzoneAcl withScope(final AclScope scope) {
    return scope == aclScope ? this
        : new OzoneAcl(type, name, scope, aclBits);
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
    return OzoneAcl.of(aclType, parts[1], aclScope, acls);
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
        .setRights(acl.getAclByteString());
    return builder.build();
  }

  public static OzoneAcl fromProtobuf(OzoneAclInfo protoAcl) {
    final byte[] bytes = protoAcl.getRights().toByteArray();
    if (bytes.length > 4) {
      throw new AssertionError("Expected at most 4 bytes but got " + bytes.length);
    }
    int aclRights = 0;
    for (int i = 0; i < bytes.length; i++) {
      aclRights |= (bytes[i] & 0xff) << (i * 8);
    }
    return new OzoneAcl(ACLIdentityType.valueOf(protoAcl.getType().name()), protoAcl.getName(),
        AclScope.valueOf(protoAcl.getAclScope().name()), aclRights);
  }

  public AclScope getAclScope() {
    return aclScope;
  }

  @Override
  public String toString() {
    return toStringMethod.get();
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
    return hashCodeMethod.get();
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
    return aclBits == 0;
  }

  @VisibleForTesting
  public boolean isSet(ACLType acl) {
    return (aclBits & toInt(acl)) != 0;
  }

  public boolean checkAccess(ACLType acl) {
    return (isSet(acl) || isSet(ALL)) && !isSet(NONE);
  }

  public OzoneAcl add(OzoneAcl other) {
    return apply(bits -> bits | other.aclBits);
  }

  public OzoneAcl remove(OzoneAcl other) {
    return apply(bits -> bits & ~other.aclBits);
  }

  private OzoneAcl apply(IntFunction<Integer> op) {
    int applied = op.apply(aclBits);
    return applied == aclBits
        ? this
        : new OzoneAcl(type, name, aclScope, applied);
  }

  @JsonIgnore
  public ByteString getAclByteString() {
    // only first 9 bits are used currently
    final byte first = (byte) aclBits;
    final byte second = (byte) (aclBits >>> 8);
    final byte[] bytes = second != 0 ? new byte[]{first, second} : new byte[]{first};
    return ProtoUtils.unsafeByteString(bytes);
  }

  @JsonIgnore
  public List<String> getAclStringList() {
    return getAclList(aclBits, ACLType::name);
  }

  public List<ACLType> getAclList() {
    return getAclList(aclBits, Function.identity());
  }

  public Set<ACLType> getAclSet() {
    return Collections.unmodifiableSet(EnumSet.copyOf(getAclList()));
  }

  private static <T> List<T> getAclList(int aclBits, Function<ACLType, T> converter) {
    if (aclBits == 0) {
      return Collections.emptyList();
    }
    final List<T> toReturn = new ArrayList<>(Integer.bitCount(aclBits));
    for (int i = 0; i < ACLType.values().length; i++) {
      if ((toInt(i) & aclBits) != 0) {
        toReturn.add(converter.apply(ACLType.values()[i]));
      }
    }
    return Collections.unmodifiableList(toReturn);
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
        this.aclBits == otherAcl.aclBits &&
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

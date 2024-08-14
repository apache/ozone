/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.metadata;

import java.math.BigInteger;
import java.security.cert.X509Certificate;
import java.util.Map;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.db.ByteStringCodec;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.hdds.utils.db.StringCodec;

/**
 * Class defines the structure and types of the scm.db.
 */
public class SCMDBDefinition extends DBDefinition.WithMap {
  public SCMDBDefinition() {
    this(COLUMN_FAMILIES);
  }

  protected SCMDBDefinition(Map<String, DBColumnFamilyDefinition<?, ?>> map) {
    super(map);
  }

  public static final DBColumnFamilyDefinition<Long, DeletedBlocksTransaction>
      DELETED_BLOCKS =
      new DBColumnFamilyDefinition<>(
          "deletedBlocks",
          Long.class,
          LongCodec.get(),
          DeletedBlocksTransaction.class,
          Proto2Codec.get(DeletedBlocksTransaction.getDefaultInstance()));

  public static final DBColumnFamilyDefinition<BigInteger, X509Certificate>
      VALID_CERTS =
      new DBColumnFamilyDefinition<>(
          "validCerts",
          BigInteger.class,
          BigIntegerCodec.get(),
          X509Certificate.class,
          X509CertificateCodec.get());

  public static final DBColumnFamilyDefinition<BigInteger, X509Certificate>
      VALID_SCM_CERTS =
      new DBColumnFamilyDefinition<>(
          "validSCMCerts",
          BigInteger.class,
          BigIntegerCodec.get(),
          X509Certificate.class,
          X509CertificateCodec.get());

  public static final DBColumnFamilyDefinition<PipelineID, Pipeline>
      PIPELINES =
      new DBColumnFamilyDefinition<>(
          "pipelines",
          PipelineID.class,
          PipelineID.getCodec(),
          Pipeline.class,
          Pipeline.getCodec());

  public static final DBColumnFamilyDefinition<ContainerID, ContainerInfo>
      CONTAINERS =
      new DBColumnFamilyDefinition<>(
          "containers",
          ContainerID.class,
          ContainerID.getCodec(),
          ContainerInfo.class,
          ContainerInfo.getCodec());

  public static final DBColumnFamilyDefinition<String, TransactionInfo>
      TRANSACTIONINFO =
      new DBColumnFamilyDefinition<>(
          "scmTransactionInfos",
          String.class,
          StringCodec.get(),
          TransactionInfo.class,
          TransactionInfo.getCodec());

  public static final DBColumnFamilyDefinition<String, Long>
      SEQUENCE_ID =
      new DBColumnFamilyDefinition<>(
          "sequenceId",
          String.class,
          StringCodec.get(),
          Long.class,
          LongCodec.get());

  public static final DBColumnFamilyDefinition<ContainerID,
      MoveDataNodePair>
      MOVE =
      new DBColumnFamilyDefinition<>(
          "move",
          ContainerID.class,
          ContainerID.getCodec(),
          MoveDataNodePair.class,
          MoveDataNodePair.getCodec());

  /**
   * Stores miscellaneous SCM metadata, including upgrade finalization status
   * and metadata layout version.
   */
  public static final DBColumnFamilyDefinition<String, String>
      META = new DBColumnFamilyDefinition<>(
          "meta",
          String.class,
          StringCodec.get(),
          String.class,
          StringCodec.get());

  public static final DBColumnFamilyDefinition<String, ByteString>
      STATEFUL_SERVICE_CONFIG =
      new DBColumnFamilyDefinition<>(
          "statefulServiceConfig",
          String.class,
          StringCodec.get(),
          ByteString.class,
          ByteStringCodec.get());

  private static final Map<String, DBColumnFamilyDefinition<?, ?>>
      COLUMN_FAMILIES = DBColumnFamilyDefinition.newUnmodifiableMap(
          CONTAINERS,
          DELETED_BLOCKS,
          META,
          MOVE,
          PIPELINES,
          SEQUENCE_ID,
          STATEFUL_SERVICE_CONFIG,
          TRANSACTIONINFO,
          VALID_CERTS,
          VALID_SCM_CERTS);

  @Override
  public String getName() {
    return "scm.db";
  }

  @Override
  public String getLocationConfigKey() {
    return ScmConfigKeys.OZONE_SCM_DB_DIRS;
  }
}

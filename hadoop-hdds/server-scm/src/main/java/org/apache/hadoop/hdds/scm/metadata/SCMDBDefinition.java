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

package org.apache.hadoop.hdds.scm.metadata;

import com.google.protobuf.ByteString;
import java.math.BigInteger;
import java.security.cert.X509Certificate;
import java.util.Map;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.TransactionInfo;
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
  public static final DBColumnFamilyDefinition<Long, DeletedBlocksTransaction>
      DELETED_BLOCKS =
      new DBColumnFamilyDefinition<>(
          "deletedBlocks",
          LongCodec.get(),
          Proto2Codec.get(DeletedBlocksTransaction.getDefaultInstance()));

  public static final DBColumnFamilyDefinition<BigInteger, X509Certificate>
      VALID_CERTS =
      new DBColumnFamilyDefinition<>(
          "validCerts",
          BigIntegerCodec.get(),
          X509CertificateCodec.get());

  public static final DBColumnFamilyDefinition<BigInteger, X509Certificate>
      VALID_SCM_CERTS =
      new DBColumnFamilyDefinition<>(
          "validSCMCerts",
          BigIntegerCodec.get(),
          X509CertificateCodec.get());

  public static final DBColumnFamilyDefinition<PipelineID, Pipeline>
      PIPELINES =
      new DBColumnFamilyDefinition<>(
          "pipelines",
          PipelineID.getCodec(),
          Pipeline.getCodec());

  public static final DBColumnFamilyDefinition<ContainerID, ContainerInfo>
      CONTAINERS =
      new DBColumnFamilyDefinition<>(
          "containers",
          ContainerID.getCodec(),
          ContainerInfo.getCodec());

  public static final DBColumnFamilyDefinition<String, TransactionInfo>
      TRANSACTIONINFO =
      new DBColumnFamilyDefinition<>(
          "scmTransactionInfos",
          StringCodec.get(),
          TransactionInfo.getCodec());

  public static final DBColumnFamilyDefinition<String, Long>
      SEQUENCE_ID =
      new DBColumnFamilyDefinition<>(
          "sequenceId",
          StringCodec.get(),
          LongCodec.get());

  public static final DBColumnFamilyDefinition<ContainerID,
      MoveDataNodePair>
      MOVE =
      new DBColumnFamilyDefinition<>(
          "move",
          ContainerID.getCodec(),
          MoveDataNodePair.getCodec());

  /**
   * Stores miscellaneous SCM metadata, including upgrade finalization status
   * and metadata layout version.
   */
  public static final DBColumnFamilyDefinition<String, String>
      META = new DBColumnFamilyDefinition<>(
          "meta",
          StringCodec.get(),
          StringCodec.get());

  public static final DBColumnFamilyDefinition<String, ByteString>
      STATEFUL_SERVICE_CONFIG =
      new DBColumnFamilyDefinition<>(
          "statefulServiceConfig",
          StringCodec.get(),
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

  private static final SCMDBDefinition INSTANCE = new SCMDBDefinition(COLUMN_FAMILIES);

  public static SCMDBDefinition get() {
    return INSTANCE;
  }

  protected SCMDBDefinition(Map<String, DBColumnFamilyDefinition<?, ?>> map) {
    super(map);
  }

  @Override
  public String getName() {
    return "scm.db";
  }

  @Override
  public String getLocationConfigKey() {
    return ScmConfigKeys.OZONE_SCM_DB_DIRS;
  }
}

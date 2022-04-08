package org.apache.hadoop.hdds.scm.ha;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.utils.db.Table;

import java.io.IOException;
import java.lang.reflect.Proxy;

/**
 * This class implements methods to save and read configurations of a
 * stateful service from DB.
 */
public final class StatefulServiceStateManagerImpl
    implements StatefulServiceStateManager {

  private Table<String, ByteString> statefulServiceConfig;
  private final DBTransactionBuffer transactionBuffer;
//  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private StatefulServiceStateManagerImpl(
      Table<String, ByteString> statefulServiceConfig,
      DBTransactionBuffer scmDBTransactionBuffer) {
    this.statefulServiceConfig = statefulServiceConfig;
    this.transactionBuffer = scmDBTransactionBuffer;
  }

  @Override
  public void saveConfiguration(String serviceName,
                                Message configurationMessage)
      throws IOException {
    // do we need a write lock here?
    transactionBuffer.addToBuffer(statefulServiceConfig, serviceName,
        configurationMessage.toByteString());
  }

  @Override
  public ByteString readConfiguration(String serviceName) throws IOException {
    // do we need a read lock here?
    return statefulServiceConfig.get(serviceName);
  }

  @Override
  public void reinitialize(Table<String, ByteString> configs) {
    this.statefulServiceConfig = configs;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for StatefulServiceStateManager.
   */
  public static class Builder {
    private Table<String, ByteString> statefulServiceConfig;
    private DBTransactionBuffer transactionBuffer;
    private SCMRatisServer scmRatisServer;

    public Builder setStatefulServiceConfig(
        final Table<String, ByteString> statefulServiceConfig) {
      this.statefulServiceConfig = statefulServiceConfig;
      return this;
    }

    public Builder setSCMDBTransactionBuffer(
        final DBTransactionBuffer dbTransactionBuffer) {
      this.transactionBuffer = dbTransactionBuffer;
      return this;
    }

    public Builder setRatisServer(final SCMRatisServer ratisServer) {
      scmRatisServer = ratisServer;
      return this;
    }

    public StatefulServiceStateManager build() {
      final StatefulServiceStateManager stateManager =
          new StatefulServiceStateManagerImpl(statefulServiceConfig,
              transactionBuffer);

      final SCMHAInvocationHandler invocationHandler =
          new SCMHAInvocationHandler(RequestType.STATEFUL_SERVICE_CONFIG,
              stateManager, scmRatisServer);

      return (StatefulServiceStateManager) Proxy.newProxyInstance(
          SCMHAInvocationHandler.class.getClassLoader(),
          new Class<?>[]{StatefulServiceStateManager.class}, invocationHandler);
    }
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ipc_;

import java.lang.reflect.Proxy;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

import java.net.InetSocketAddress;
import java.io.*;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.SocketFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc_.Client.ConnectionId;
import org.apache.hadoop.ipc_.RPC.RpcInvoker;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.conf.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An RpcEngine implementation for Writable data. */
@Deprecated
public class WritableRpcEngine implements RpcEngine {
  private static final Logger LOG = LoggerFactory.getLogger(RPC.class);
  
  //writableRpcVersion should be updated if there is a change
  //in format of the rpc messages.
  
  // 2L - added declared class to Invocation
  public static final long writableRpcVersion = 2L;
  
  /**
   * Whether or not this class has been initialized.
   */
  private static boolean isInitialized = false;
  
  static { 
    ensureInitialized();
  }
  
  /**
   * Initialize this class if it isn't already.
   */
  public static synchronized void ensureInitialized() {
    if (!isInitialized) {
      initialize();
    }
  }
  
  /**
   * Register the rpcRequest deserializer for WritableRpcEngine
   */
  private static synchronized void initialize() {
    org.apache.hadoop.ipc_.Server.registerProtocolEngine(RPC.RpcKind.RPC_WRITABLE,
        Invocation.class, new Server.WritableRpcInvoker());
    isInitialized = true;
  }

  
  /** A method invocation, including the method name and its parameters.*/
  private static class Invocation implements Writable, Configurable {
    private String methodName;
    private Class<?>[] parameterClasses;
    private Object[] parameters;
    private Configuration conf;
    private long clientVersion;
    private int clientMethodsHash;
    private String declaringClassProtocolName;
    
    //This could be different from static writableRpcVersion when received
    //at server, if client is using a different version.
    private long rpcVersion;

    @SuppressWarnings("unused") // called when deserializing an invocation
    public Invocation() {}

    public Invocation(Method method, Object[] parameters) {
      this.methodName = method.getName();
      this.parameterClasses = method.getParameterTypes();
      this.parameters = parameters;
      rpcVersion = writableRpcVersion;
      if (method.getDeclaringClass().equals(VersionedProtocol.class)) {
        //VersionedProtocol is exempted from version check.
        clientVersion = 0;
        clientMethodsHash = 0;
      } else {
        this.clientVersion = RPC.getProtocolVersion(method.getDeclaringClass());
        this.clientMethodsHash = ProtocolSignature.getFingerprint(method
            .getDeclaringClass().getMethods());
      }
      this.declaringClassProtocolName = 
          RPC.getProtocolName(method.getDeclaringClass());
    }

    /** The name of the method invoked. */
    public String getMethodName() { return methodName; }

    /** The parameter classes. */
    public Class<?>[] getParameterClasses() { return parameterClasses; }

    /** The parameter instances. */
    public Object[] getParameters() { return parameters; }
    
    private long getProtocolVersion() {
      return clientVersion;
    }

    @SuppressWarnings("unused")
    private int getClientMethodsHash() {
      return clientMethodsHash;
    }
    
    /**
     * Returns the rpc version used by the client.
     * @return rpcVersion
     */
    public long getRpcVersion() {
      return rpcVersion;
    }

    @Override
    @SuppressWarnings("deprecation")
    public void readFields(DataInput in) throws IOException {
      rpcVersion = in.readLong();
      declaringClassProtocolName = UTF8.readString(in);
      methodName = UTF8.readString(in);
      clientVersion = in.readLong();
      clientMethodsHash = in.readInt();
      parameters = new Object[in.readInt()];
      parameterClasses = new Class[parameters.length];
      ObjectWritable objectWritable = new ObjectWritable();
      for (int i = 0; i < parameters.length; i++) {
        parameters[i] = 
            ObjectWritable.readObject(in, objectWritable, this.conf);
        parameterClasses[i] = objectWritable.getDeclaredClass();
      }
    }

    @Override
    @SuppressWarnings("deprecation")
    public void write(DataOutput out) throws IOException {
      out.writeLong(rpcVersion);
      UTF8.writeString(out, declaringClassProtocolName);
      UTF8.writeString(out, methodName);
      out.writeLong(clientVersion);
      out.writeInt(clientMethodsHash);
      out.writeInt(parameterClasses.length);
      for (int i = 0; i < parameterClasses.length; i++) {
        ObjectWritable.writeObject(out, parameters[i], parameterClasses[i],
                                   conf, true);
      }
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append(methodName);
      buffer.append("(");
      for (int i = 0; i < parameters.length; i++) {
        if (i != 0)
          buffer.append(", ");
        buffer.append(parameters[i]);
      }
      buffer.append(")");
      buffer.append(", rpc version="+rpcVersion);
      buffer.append(", client version="+clientVersion);
      buffer.append(", methodsFingerPrint="+clientMethodsHash);
      return buffer.toString();
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Configuration getConf() {
      return this.conf;
    }

  }

  private static ClientCache CLIENTS=new ClientCache();
  
  private static class Invoker implements RpcInvocationHandler {
    private Client.ConnectionId remoteId;
    private Client client;
    private boolean isClosed = false;
    private final AtomicBoolean fallbackToSimpleAuth;
    private final AlignmentContext alignmentContext;

    public Invoker(Class<?> protocol,
                   InetSocketAddress address, UserGroupInformation ticket,
                   Configuration conf, SocketFactory factory,
                   int rpcTimeout, AtomicBoolean fallbackToSimpleAuth,
                   AlignmentContext alignmentContext)
        throws IOException {
      this.remoteId = Client.ConnectionId.getConnectionId(address, protocol,
          ticket, rpcTimeout, null, conf);
      this.client = CLIENTS.getClient(conf, factory);
      this.fallbackToSimpleAuth = fallbackToSimpleAuth;
      this.alignmentContext = alignmentContext;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
      long startTime = 0;
      if (LOG.isDebugEnabled()) {
        startTime = Time.monotonicNow();
      }

      ObjectWritable value = (ObjectWritable)
        client.call(RPC.RpcKind.RPC_WRITABLE, new Invocation(method, args),
          remoteId, fallbackToSimpleAuth, alignmentContext);
      if (LOG.isDebugEnabled()) {
        long callTime = Time.monotonicNow() - startTime;
        LOG.debug("Call: " + method.getName() + " " + callTime);
      }
      return value.get();
    }
    
    /* close the IPC client that's responsible for this invoker's RPCs */ 
    @Override
    synchronized public void close() {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }

    @Override
    public ConnectionId getConnectionId() {
      return remoteId;
    }
  }
  
  // for unit testing only
  static Client getClient(Configuration conf) {
    return CLIENTS.getClient(conf);
  }
  
  /**
   * Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. 
   * @param <T> Generics Type T
   * @param protocol input protocol.
   * @param clientVersion input clientVersion.
   * @param addr input addr.
   * @param ticket input ticket.
   * @param conf input configuration.
   * @param factory input factory.
   * @param rpcTimeout input rpcTimeout.
   * @param connectionRetryPolicy input connectionRetryPolicy.
   * @throws IOException raised on errors performing I/O.
   */
  @Override
  public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
                         InetSocketAddress addr, UserGroupInformation ticket,
                         Configuration conf, SocketFactory factory,
                         int rpcTimeout, RetryPolicy connectionRetryPolicy)
    throws IOException {
    return getProxy(protocol, clientVersion, addr, ticket, conf, factory,
      rpcTimeout, connectionRetryPolicy, null, null);
  }

  /**
   * Construct a client-side proxy object with a ConnectionId.
   *
   * @param <T> Generics Type T.
   * @param protocol input protocol.
   * @param clientVersion input clientVersion.
   * @param connId input ConnectionId.
   * @param conf input Configuration.
   * @param factory input factory.
   * @throws IOException raised on errors performing I/O.
   * @return ProtocolProxy.
   */
  @Override
  public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
      Client.ConnectionId connId, Configuration conf, SocketFactory factory)
      throws IOException {
    return getProxy(protocol, clientVersion, connId.getAddress(),
        connId.getTicket(), conf, factory, connId.getRpcTimeout(),
        connId.getRetryPolicy(), null, null);
  }

  /**
   * Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. 
   * @param <T> Generics Type.
   * @param protocol input protocol.
   * @param clientVersion input clientVersion.
   * @param addr input addr.
   * @param ticket input ticket.
   * @param conf input configuration.
   * @param factory input factory.
   * @param rpcTimeout input rpcTimeout.
   * @param connectionRetryPolicy input connectionRetryPolicy.
   * @param fallbackToSimpleAuth input fallbackToSimpleAuth.
   * @param alignmentContext input alignmentContext.
   * @return ProtocolProxy.
   */
  @Override
  @SuppressWarnings("unchecked")
  public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
                         InetSocketAddress addr, UserGroupInformation ticket,
                         Configuration conf, SocketFactory factory,
                         int rpcTimeout, RetryPolicy connectionRetryPolicy,
                         AtomicBoolean fallbackToSimpleAuth,
                         AlignmentContext alignmentContext)
    throws IOException {    

    if (connectionRetryPolicy != null) {
      throw new UnsupportedOperationException(
          "Not supported: connectionRetryPolicy=" + connectionRetryPolicy);
    }

    T proxy = (T) Proxy.newProxyInstance(protocol.getClassLoader(),
        new Class[] { protocol }, new Invoker(protocol, addr, ticket, conf,
            factory, rpcTimeout, fallbackToSimpleAuth, alignmentContext));
    return new ProtocolProxy<T>(protocol, proxy, true);
  }
  
  /* Construct a server for a protocol implementation instance listening on a
   * port and address. */
  @Override
  public RPC.Server getServer(Class<?> protocolClass,
                      Object protocolImpl, String bindAddress, int port,
                      int numHandlers, int numReaders, int queueSizePerHandler,
                      boolean verbose, Configuration conf,
                      SecretManager<? extends TokenIdentifier> secretManager,
                      String portRangeConfig, AlignmentContext alignmentContext)
    throws IOException {
    return new Server(protocolClass, protocolImpl, conf, bindAddress, port,
        numHandlers, numReaders, queueSizePerHandler, verbose, secretManager,
        portRangeConfig, alignmentContext);
  }


  /** An RPC Server. */
  @Deprecated
  public static class Server extends RPC.Server {
    /** 
     * Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * 
     * @deprecated Use #Server(Class, Object, Configuration, String, int)
     * @throws IOException raised on errors performing I/O.
     */
    @Deprecated
    public Server(Object instance, Configuration conf, String bindAddress,
        int port) throws IOException {
      this(null, instance, conf,  bindAddress, port);
    }
    
    
    /** Construct an RPC server.
     * @param protocolClass class
     * @param protocolImpl the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @throws IOException raised on errors performing I/O.
     */
    public Server(Class<?> protocolClass, Object protocolImpl, 
        Configuration conf, String bindAddress, int port) 
      throws IOException {
      this(protocolClass, protocolImpl, conf,  bindAddress, port, 1, -1, -1,
          false, null, null);
    }
    
    /** 
     * Construct an RPC server.
     * @param protocolImpl the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     * @param numReaders input numberReaders.
     * @param queueSizePerHandler input queueSizePerHandler.
     * @param secretManager input secretManager.
     * 
     * @deprecated use Server#Server(Class, Object, 
     *      Configuration, String, int, int, int, int, boolean, SecretManager)
     * @throws IOException raised on errors performing I/O.
     */
    @Deprecated
    public Server(Object protocolImpl, Configuration conf, String bindAddress,
        int port, int numHandlers, int numReaders, int queueSizePerHandler,
        boolean verbose, SecretManager<? extends TokenIdentifier> secretManager) 
            throws IOException {
       this(null, protocolImpl,  conf,  bindAddress,   port,
                   numHandlers,  numReaders,  queueSizePerHandler,  verbose, 
                   secretManager, null);
   
    }
    
    /** 
     * Construct an RPC server.
     * @param protocolClass - the protocol being registered
     *     can be null for compatibility with old usage (see below for details)
     * @param protocolImpl the protocol impl that will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     * @param secretManager input secretManager.
     * @param queueSizePerHandler input queueSizePerHandler.
     * @param portRangeConfig input portRangeConfig.
     * @param numReaders input numReaders.
     *
     * @deprecated use Server#Server(Class, Object,
     *      Configuration, String, int, int, int, int, boolean, SecretManager)
     * @throws IOException raised on errors performing I/O.
     */
    @Deprecated
    public Server(Class<?> protocolClass, Object protocolImpl,
        Configuration conf, String bindAddress,  int port,
        int numHandlers, int numReaders, int queueSizePerHandler, 
        boolean verbose, SecretManager<? extends TokenIdentifier> secretManager,
        String portRangeConfig) 
        throws IOException {
      this(null, protocolImpl,  conf,  bindAddress,   port,
          numHandlers,  numReaders,  queueSizePerHandler,  verbose,
          secretManager, null, null);
    }

    /**
     * Construct an RPC server.
     * @param protocolClass - the protocol being registered
     *     can be null for compatibility with old usage (see below for details)
     * @param protocolImpl the protocol impl that will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     * @param alignmentContext provides server state info on client responses
     * @param numReaders input numReaders.
     * @param portRangeConfig input portRangeConfig.
     * @param queueSizePerHandler input queueSizePerHandler.
     * @param secretManager input secretManager.
     * @throws IOException raised on errors performing I/O.
     */
    public Server(Class<?> protocolClass, Object protocolImpl,
        Configuration conf, String bindAddress,  int port,
        int numHandlers, int numReaders, int queueSizePerHandler,
        boolean verbose, SecretManager<? extends TokenIdentifier> secretManager,
        String portRangeConfig, AlignmentContext alignmentContext)
        throws IOException {
      super(bindAddress, port, null, numHandlers, numReaders,
          queueSizePerHandler, conf,
          serverNameFromClass(protocolImpl.getClass()), secretManager,
          portRangeConfig);
      setAlignmentContext(alignmentContext);
      this.verbose = verbose;
      
      
      Class<?>[] protocols;
      if (protocolClass == null) { // derive protocol from impl
        /*
         * In order to remain compatible with the old usage where a single
         * target protocolImpl is suppled for all protocol interfaces, and
         * the protocolImpl is derived from the protocolClass(es) 
         * we register all interfaces extended by the protocolImpl
         */
        protocols = RPC.getProtocolInterfaces(protocolImpl.getClass());

      } else {
        if (!protocolClass.isAssignableFrom(protocolImpl.getClass())) {
          throw new IOException("protocolClass "+ protocolClass +
              " is not implemented by protocolImpl which is of class " +
              protocolImpl.getClass());
        }
        // register protocol class and its super interfaces
        registerProtocolAndImpl(RPC.RpcKind.RPC_WRITABLE, protocolClass, protocolImpl);
        protocols = RPC.getProtocolInterfaces(protocolClass);
      }
      for (Class<?> p : protocols) {
        if (!p.equals(VersionedProtocol.class)) {
          registerProtocolAndImpl(RPC.RpcKind.RPC_WRITABLE, p, protocolImpl);
        }
      }

    }

    private static void log(String value) {
      if (value!= null && value.length() > 55)
        value = value.substring(0, 55)+"...";
      LOG.info(value);
    }

    @Deprecated
    static class WritableRpcInvoker implements RpcInvoker {

     @Override
      public Writable call(org.apache.hadoop.ipc_.RPC.Server server,
          String protocolName, Writable rpcRequest, long receivedTime)
          throws IOException, RPC.VersionMismatch {

        Invocation call = (Invocation)rpcRequest;
        if (server.verbose) log("Call: " + call);

        // Verify writable rpc version
        if (call.getRpcVersion() != writableRpcVersion) {
          // Client is using a different version of WritableRpc
          throw new RpcServerException(
              "WritableRpc version mismatch, client side version="
                  + call.getRpcVersion() + ", server side version="
                  + writableRpcVersion);
        }

        long clientVersion = call.getProtocolVersion();
        final String protoName;
        ProtoClassProtoImpl protocolImpl;
        if (call.declaringClassProtocolName.equals(VersionedProtocol.class.getName())) {
          // VersionProtocol methods are often used by client to figure out
          // which version of protocol to use.
          //
          // Versioned protocol methods should go the protocolName protocol
          // rather than the declaring class of the method since the
          // the declaring class is VersionedProtocol which is not 
          // registered directly.
          // Send the call to the highest  protocol version
          VerProtocolImpl highest = server.getHighestSupportedProtocol(
              RPC.RpcKind.RPC_WRITABLE, protocolName);
          if (highest == null) {
            throw new RpcServerException("Unknown protocol: " + protocolName);
          }
          protocolImpl = highest.protocolTarget;
        } else {
          protoName = call.declaringClassProtocolName;

          // Find the right impl for the protocol based on client version.
          ProtoNameVer pv = 
              new ProtoNameVer(call.declaringClassProtocolName, clientVersion);
          protocolImpl = 
              server.getProtocolImplMap(RPC.RpcKind.RPC_WRITABLE).get(pv);
          if (protocolImpl == null) { // no match for Protocol AND Version
             VerProtocolImpl highest = 
                 server.getHighestSupportedProtocol(RPC.RpcKind.RPC_WRITABLE, 
                     protoName);
            if (highest == null) {
              throw new RpcServerException("Unknown protocol: " + protoName);
            } else { // protocol supported but not the version that client wants
              throw new RPC.VersionMismatch(protoName, clientVersion,
                highest.version);
            }
          }
        }

        // Invoke the protocol method
        Exception exception = null;
        Call currentCall = Server.getCurCall().get();
        try {
          Method method =
              protocolImpl.protocolClass.getMethod(call.getMethodName(),
              call.getParameterClasses());
          method.setAccessible(true);
          server.rpcDetailedMetrics.init(protocolImpl.protocolClass);
          currentCall.setDetailedMetricsName(call.getMethodName());
          Object value = 
              method.invoke(protocolImpl.protocolImpl, call.getParameters());
          if (server.verbose) log("Return: "+value);
          return new ObjectWritable(method.getReturnType(), value);

        } catch (InvocationTargetException e) {
          Throwable target = e.getTargetException();
          if (target instanceof IOException) {
            exception = (IOException)target;
            throw (IOException)target;
          } else {
            IOException ioe = new IOException(target.toString());
            ioe.setStackTrace(target.getStackTrace());
            exception = ioe;
            throw ioe;
          }
        } catch (Throwable e) {
          if (!(e instanceof IOException)) {
            LOG.error("Unexpected throwable object ", e);
          }
          IOException ioe = new IOException(e.toString());
          ioe.setStackTrace(e.getStackTrace());
          exception = ioe;
          throw ioe;
        } finally {
          if (exception != null) {
            currentCall.setDetailedMetricsName(
                exception.getClass().getSimpleName());
          }
        }
      }
    }
  }

  @Override
  public ProtocolProxy<ProtocolMetaInfoPB> getProtocolMetaInfoProxy(
      ConnectionId connId, Configuration conf, SocketFactory factory)
      throws IOException {
    throw new UnsupportedOperationException("This proxy is not supported");
  }
}

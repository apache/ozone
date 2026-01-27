package org.apache.hadoop.ozone.om.request.invocation;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcConstants;
import org.apache.hadoop.ipc.RpcInvocationHandler;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFollowerReadFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProviderBase;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysArgs;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

/**
 * An InvocationHandler that handles retries and failovers for OzoneManager.
 */
public class OzoneRetryInvocationHandler<T> implements RpcInvocationHandler {

  public static final Logger LOG = LoggerFactory.getLogger(OzoneRetryInvocationHandler.class);

  private ThreadLocal<OMRequest> omRequest;

  private final ProxyDescriptor<T> proxyDescriptor;

  private volatile boolean hasSuccessfulCall = false;

  private HashSet<String> failedAtLeastOnce = new HashSet<>();

  private final RetryPolicy defaultPolicy;
  private final Map<String, RetryPolicy> methodNameToPolicyMap;

  private final AsyncCallHandler asyncCallHandler = new AsyncCallHandler();

  private final OMFailoverProxyProviderBase<?> omFailoverProxyProvider;

  public OzoneRetryInvocationHandler(FailoverProxyProvider<T> proxyProvider,
                                     RetryPolicy retryPolicy) {
    this(proxyProvider, retryPolicy, Collections.<String, RetryPolicy>emptyMap());
  }

  protected OzoneRetryInvocationHandler(FailoverProxyProvider<T> proxyProvider,
                                   RetryPolicy defaultPolicy,
                                   Map<String, RetryPolicy> methodNameToPolicyMap) {
    this.proxyDescriptor = new ProxyDescriptor<>(proxyProvider);
    this.defaultPolicy = defaultPolicy;
    this.methodNameToPolicyMap = methodNameToPolicyMap;
    this.omFailoverProxyProvider = resolveOMFailoverProxyProvider(proxyProvider);
  }

  private static OMFailoverProxyProviderBase<?> resolveOMFailoverProxyProvider(
      FailoverProxyProvider<?> proxyProvider) {
    if (proxyProvider instanceof OMFailoverProxyProviderBase) {
      return (OMFailoverProxyProviderBase<?>) proxyProvider;
    }
    if (proxyProvider instanceof HadoopRpcOMFollowerReadFailoverProxyProvider) {
      return ((HadoopRpcOMFollowerReadFailoverProxyProvider) proxyProvider)
          .getFailoverProxy();
    }
    return null;
  }

  public RetryPolicy getRetryPolicy(Method method) {
    final RetryPolicy policy = methodNameToPolicyMap.get(method.getName());
    return policy != null ? policy : defaultPolicy;
  }

  public long getFailoverCount() {
    return proxyDescriptor.getFailoverCount();
  }

  public ProxyDescriptor<T> getProxyDescriptor() {
    return proxyDescriptor;
  }

  public AsyncCallHandler getAsyncCallHandler() {
    return asyncCallHandler;
  }

  private Call newCall(Method method, Object[] args, boolean isRpc,
                                              int callId) {
    if (Client.isAsynchronousMode()) {
      return asyncCallHandler.newAsyncCall(method, args, isRpc, callId, this);
    } else {
      return new Call(method, args, isRpc, callId, this);
    }
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
    final boolean isRpc = isRpcInvocation(proxyDescriptor.getProxy());
    final int callId = isRpc ? Client.nextCallId() : RpcConstants.INVALID_CALL_ID;

    final Call call = newCall(method, args, isRpc, callId);
    while (true) {
      final CallReturn c = call.invokeOnce();
      final CallReturn.State state = c.getState();
      if (state == CallReturn.State.ASYNC_INVOKED) {
        return null; // return null for async calls
      } else if (c.getState() != CallReturn.State.RETRY) {
        return c.getReturnValue();
      } else {
        if (omFailoverProxyProvider != null) {
          OMRequest request = omFailoverProxyProvider.getOmRequest();
          Object[] args1 = call.getArgs();
          for (int i = 0; i < args1.length; i++) {
            if (args1[i] instanceof OMRequest) {
              args1[i] = request;
            }
          }
        }
      }
    }
  }

  public RetryInfo handleException(final Method method, final int callId,
                                                           final RetryPolicy policy, final Counters counters,
                                                           final long expectFailoverCount, final Exception e)
      throws Exception {
    final RetryInfo retryInfo = RetryInfo.newRetryInfo(policy, e,
        counters, proxyDescriptor.idempotentOrAtMostOnce(method),
        expectFailoverCount);
    if (retryInfo.isFail()) {
      // fail.
      if (retryInfo.getAction().reason != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Exception while invoking call #{} {}. Not retrying because {}", callId,
              proxyDescriptor.getProxyInfo().getString(method.getName()), retryInfo.getAction().reason, e);
        }
      }
      throw retryInfo.getFailException();
    }

    log(method, retryInfo.isFailover(), counters.getFailovers(), counters.getRetries(), retryInfo.getDelay(), e);
    return retryInfo;
  }

  private void log(final Method method, final boolean isFailover, final int failovers,
                   final int retries, final long delay, final Exception ex) {
    boolean info = true;
    // If this is the first failover to this proxy, skip logging at INFO level
    if (!failedAtLeastOnce.contains(proxyDescriptor.getProxyInfo().toString())) {
      failedAtLeastOnce.add(proxyDescriptor.getProxyInfo().toString());

      // If successful calls were made to this proxy, log info even for first
      // failover
      info = hasSuccessfulCall || asyncCallHandler.hasSuccessfulCall();
      if (!info && !LOG.isDebugEnabled()) {
        return;
      }
    }

    final StringBuilder b = new StringBuilder()
        .append(ex)
        .append(", while invoking ")
        .append(proxyDescriptor.getProxyInfo().getString(method.getName()));
    if (failovers > 0) {
      b.append(" after ").append(failovers).append(" failover attempts");
    }
    b.append(isFailover ? ". Trying to failover " : ". Retrying ");
    b.append(delay > 0 ? "after sleeping for " + delay + "ms." : "immediately.");
    b.append(" Current retry count: ").append(retries).append(".");

    if (info) {
      LOG.info(b.toString());
    } else {
      LOG.debug(b.toString(), ex);
    }
  }

  protected Object invokeMethod(Method method, Object[] args) throws Throwable {
    try {
      if (!method.isAccessible()) {
        method.setAccessible(true);
      }
      T proxy = null;
      if (args.length == 2 && args[1] instanceof OMRequest && omFailoverProxyProvider != null) {
        String bucketPath = omFailoverProxyProvider
            .getWriteRequestBucketPath((OMRequest) args[1]);
        if (bucketPath != null) {
          proxy = (T) omFailoverProxyProvider.selectProxyInfo(bucketPath);
        }
        if (proxy == null) {
          proxy = proxyDescriptor.getProxy();
        }
      } else {
        proxy = proxyDescriptor.getProxy();
      }
      final Object r = method.invoke(proxy, args);
      hasSuccessfulCall = true;
      return r;
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  @VisibleForTesting
  static boolean isRpcInvocation(Object proxy) {
    if (proxy instanceof ProtocolTranslator) {
      proxy = ((ProtocolTranslator) proxy).getUnderlyingProxyObject();
    }
    if (!Proxy.isProxyClass(proxy.getClass())) {
      return false;
    }
    final InvocationHandler ih = Proxy.getInvocationHandler(proxy);
    return ih instanceof RpcInvocationHandler;
  }

  @Override
  public void close() throws IOException {
    proxyDescriptor.close();
  }

  @Override //RpcInvocationHandler
  public Client.ConnectionId getConnectionId() {
    return RPC.getConnectionIdForProxy(proxyDescriptor.getProxy());
  }

  @VisibleForTesting
  public FailoverProxyProvider<T> getProxyProvider() {
    return proxyDescriptor.getFpp();
  }

}

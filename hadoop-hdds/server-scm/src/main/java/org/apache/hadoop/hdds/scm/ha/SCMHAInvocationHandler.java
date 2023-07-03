/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.scm.metadata.Replicate;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InvocationHandler which checks for {@link Replicate} annotation and
 * dispatches the request to Ratis Server.
 */
public class SCMHAInvocationHandler implements InvocationHandler {


  private static final Logger LOG = LoggerFactory
      .getLogger(SCMHAInvocationHandler.class);

  private final RequestType requestType;
  private final Object localHandler;
  private final SCMRatisServer ratisHandler;

  /**
   * TODO.
   */
  public SCMHAInvocationHandler(final RequestType requestType,
                                final Object localHandler,
                                final SCMRatisServer ratisHandler) {
    this.requestType = requestType;
    this.localHandler = localHandler;
    this.ratisHandler = ratisHandler;
    if (ratisHandler != null) {
      ratisHandler.registerStateMachineHandler(requestType, localHandler);
    }
  }

  @Override
  public Object invoke(final Object proxy, final Method method,
                       final Object[] args) throws SCMException {
    // Javadoc for InvocationHandler#invoke specifies that args will be null
    // if the method takes no arguments. Convert this to an empty array for
    // easier handling.
    Object[] convertedArgs = (args == null) ? new Object[]{} : args;
    long startTime = Time.monotonicNow();
    final Object result =
        ratisHandler != null && method.isAnnotationPresent(Replicate.class) ?
            invokeRatis(method, convertedArgs) :
            invokeLocal(method, convertedArgs);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Call: {} took {} ms", method, Time.monotonicNow() - startTime);
    }
    return result;
  }

  /**
   * TODO.
   */
  private Object invokeLocal(Method method, Object[] args)
      throws SCMException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Invoking method {} on target {} with arguments {}",
          method, localHandler, args);
    }
    try {
      return method.invoke(localHandler, args);
    } catch (Exception e) {
      throw translateException(e);
    }
  }

  /**
   * TODO.
   */
  private Object invokeRatis(Method method, Object[] args)
      throws SCMException {
    try {
      return invokeRatisImpl(method, args);
    } catch (Exception e) {
      throw translateException(e);
    }
  }

  private Object invokeRatisImpl(Method method, Object[] args)
      throws Exception {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Invoking method {} on target {}", method, ratisHandler);
    }
    // TODO: Add metric here to track time taken by Ratis
    Preconditions.checkNotNull(ratisHandler);
    SCMRatisRequest scmRatisRequest = SCMRatisRequest.of(requestType,
        method.getName(), method.getParameterTypes(), args);

    // Scm Cert DB updates should use RaftClient.
    // As rootCA which is primary SCM only can issues certificates to sub-CA.
    // In case primary is not leader SCM, still sub-ca cert DB updates should go
    // via ratis. So, in this special scenario we use RaftClient.
    final SCMRatisResponse response;
    if (method.getName().equals("storeValidCertificate") &&
        args[args.length - 1].equals(HddsProtos.NodeType.SCM)) {
      response =
          HASecurityUtils.submitScmCertsToRatis(
              ratisHandler.getDivision().getGroup(),
              ratisHandler.getGrpcTlsConfig(),
              scmRatisRequest.encode());

    } else {
      response = ratisHandler.submitRequest(
          scmRatisRequest);
    }

    if (response.isSuccess()) {
      return response.getResult();
    }
    // Should we unwrap and throw proper exception from here?
    throw response.getException();
  }

  private static SCMException translateException(Throwable t) {
    if (t instanceof SCMException) {
      return (SCMException) t;
    }
    if (t instanceof ExecutionException
        || t instanceof InvocationTargetException) {
      return translateException(t.getCause());
    }

    ResultCodes result;
    if (t instanceof TimeoutException) {
      result = ResultCodes.TIMEOUT;
    } else if (t instanceof NotLeaderException) {
      result = ResultCodes.SCM_NOT_LEADER;
    } else if (t instanceof IOException) {
      result = ResultCodes.IO_EXCEPTION;
    } else {
      result = ResultCodes.INTERNAL_ERROR;
    }

    return new SCMException(t, result);
  }

}

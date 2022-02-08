package org.apache.hadoop.ozone.om.request.validation;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;

import static org.apache.hadoop.ozone.om.request.validation.RequestProcessingPhase.POST_PROCESS;
import static org.apache.hadoop.ozone.om.request.validation.RequestProcessingPhase.PRE_PROCESS;

public class RequestValidations {

  private static final String DEFAULT_PACKAGE = "org.apache.hadoop.ozone";

  private String validationsPackageName = DEFAULT_PACKAGE;
  private ValidationContext context = ValidationContext.of(null, -1);
  private ValidatorRegistry registry = ValidatorRegistry.emptyRegistry();

  public RequestValidations fromPackage(String packageName) {
    validationsPackageName = packageName;
    return this;
  }

  public RequestValidations withinContext(ValidationContext context) {
    this.context = context;
    return this;
  }

  public synchronized RequestValidations load() {
    registry = new ValidatorRegistry(validationsPackageName);
    return this;
  }

  public OMRequest validateRequest(OMRequest request) throws ServiceException {
    List<Method> validations = registry.validationsFor(
        conditions(request), request.getCmdType(), PRE_PROCESS);

    OMRequest validatedRequest = request.toBuilder().build();
    try {
      for (Method m : validations) {
        if (m.getParameterCount() == 2) { // context aware
          return  (OMRequest) m.invoke(null, validatedRequest, context);
        } else {
          return (OMRequest) m.invoke(null, validatedRequest);
        }
      }
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new ServiceException(e);
    }
    // this should not happen, as parameter count is enforced by tests,
    // but for sanity return the request as is.
    return validatedRequest;
  }

  public OMResponse validateResponse(OMRequest request, OMResponse response)
      throws ServiceException {
    List<Method> validations = registry.validationsFor(
        conditions(request), request.getCmdType(), POST_PROCESS);

    OMResponse validatedResponse = response.toBuilder().build();
    try {
      for (Method m : validations) {
        if (m.getParameterCount() == 3) { // context aware post processor
          return (OMResponse) m.invoke(null, request, response, context);
        } else {
          return (OMResponse) m.invoke(null, request, response);
        }
      }
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new ServiceException(e);
    }
    // this should not happen, as parameter count is enforced by tests,
    // but for sanity return the response as is.
    return validatedResponse;
  }

  private List<ValidationCondition> conditions(OMRequest request) {
    List<ValidationCondition> conditions = new LinkedList<>();
    conditions.add(ValidationCondition.UNCONDITIONAL);
    if (context.serverVersion() != request.getVersion()) {
      if (context.serverVersion() < request.getVersion()) {
        conditions.add(ValidationCondition.NEWER_CLIENT_REQUESTS);
      } else {
        conditions.add(ValidationCondition.OLDER_CLIENT_REQUESTS);
      }
    }
    if (context.versionManager() != null
        && context.versionManager().needsFinalization()) {
      conditions.add(ValidationCondition.CLUSTER_NEEDS_FINALIZATION);
    }
    return conditions;
  }

}

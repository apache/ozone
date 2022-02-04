package org.apache.hadoop.ozone.om.request.validation;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RequestFeatureValidator {

  ValidationCondition[] conditions();

  RequestProcessingPhase processingPhase();

  Type requestType();

  boolean contextAware() default false;

}

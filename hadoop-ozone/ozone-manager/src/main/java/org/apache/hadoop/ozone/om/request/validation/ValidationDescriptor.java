package org.apache.hadoop.ozone.om.request.validation;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ValidationDescriptor {

  ValidatorType type() default ValidatorType.GENERIC_VALIDATOR;

  Type requestType();
}

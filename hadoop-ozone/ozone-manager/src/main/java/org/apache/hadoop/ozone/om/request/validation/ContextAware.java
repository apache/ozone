package org.apache.hadoop.ozone.om.request.validation;

public interface ContextAware {

  void setContext(ValidationContext context);
}

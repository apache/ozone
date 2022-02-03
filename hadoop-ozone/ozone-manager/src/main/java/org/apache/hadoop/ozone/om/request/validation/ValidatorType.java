package org.apache.hadoop.ozone.om.request.validation;

public enum ValidatorType {
  PRE_FINALIZATION_VALIDATOR,
  OLDER_CLIENT_REQUESTS_VALIDATOR,
  NEWER_CLIENT_REQUESTS_VALIDATOR,
  GENERIC_VALIDATOR
}

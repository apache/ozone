package org.apache.hadoop.ozone.om.request.validation;

public enum ValidationCondition {
  CLUSTER_IS_PRE_FINALIZED,
  OLDER_CLIENT_REQUESTS,
  NEWER_CLIENT_REQUESTS,
  UNCONDITIONAL;
}

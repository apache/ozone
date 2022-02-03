package org.apache.hadoop.ozone.om.request.validation;

public interface RequestFeatureValidator<REQTYPE> {

  REQTYPE rejectOrAdjust(REQTYPE request) throws Exception;
}

package org.apache.hadoop.ozone.s3.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

/**
 * Class that represents BadRequestException.
 */
public class BadRequestExceptionMapper implements
    ExceptionMapper<BadRequestException> {

  private static final Logger LOG =
      LoggerFactory.getLogger(BadRequestExceptionMapper.class);
  @Override
  public Response toResponse(BadRequestException exception) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Returning exception. ex: {}", exception.toString());
    }

    return Response.status(Response.Status.BAD_REQUEST)
        .entity(exception.getMessage()).build();
  }
}

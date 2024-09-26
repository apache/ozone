package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_LOG_OFFSET;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_RECON_LOG_OFFSET;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_LOG_LINES;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_RECON_LOG_LINES;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_LOG_DIRECTION;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_RECON_LOG_DIRECTION;


@Path("/log")
@Produces(MediaType.APPLICATION_JSON)
@AdminOnly
public class LogEndpoint {

  private final LogFetcher;
  private final OzoneConfiguration ozoneConfiguration;

  @Inject
  public LogEndpoint(OzoneConfiguration ozoneConfiguration) {
    this.ozoneConfiguration = ozoneConfiguration;
  }

  /**
   * Fetches the logs line by line for
   *
   * @param offset     Stores the last log line that was read
   * @param lines      Stores the number of lines to fetch from the log
   * @param direction  Stores the direction in which to fetch the logs
   *                   i.e. whether to fetch next lines or previous lines.
   *
   * @return {@link Response} of the following format
   */
  @POST
  @Path("/read")
  public Response getLogLines(
    @DefaultValue(DEFAULT_RECON_LOG_OFFSET) @QueryParam(RECON_LOG_OFFSET)
      int offset,
    @DefaultValue(DEFAULT_RECON_LOG_LINES) @QueryParam(RECON_LOG_LINES)
      int lines,
    @DefaultValue(DEFAULT_RECON_LOG_DIRECTION) @QueryParam(RECON_LOG_DIRECTION)
      int direction
  ) {

  }

}

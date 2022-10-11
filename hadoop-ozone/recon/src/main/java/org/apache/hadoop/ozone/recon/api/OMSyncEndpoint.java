package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.ozone.recon.spi.OzoneManagerServiceProvider;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Endpoint to trigger the OM DB sync between Recon and OM.
 */
@Path("/omsync")
@Produces(MediaType.APPLICATION_JSON)
public class OMSyncEndpoint {

  private OzoneManagerServiceProvider ozoneManagerServiceProvider;

  @Inject
  public OMSyncEndpoint(
      OzoneManagerServiceProvider ozoneManagerServiceProvider) {
    this.ozoneManagerServiceProvider = ozoneManagerServiceProvider;
  }

  @GET
  @Path("trigger")
  public Response triggerOMSync() {
    boolean isSuccess
        = ozoneManagerServiceProvider.triggerSyncDataFromOMImmediately();
    return Response.ok(isSuccess).build();
  }
}

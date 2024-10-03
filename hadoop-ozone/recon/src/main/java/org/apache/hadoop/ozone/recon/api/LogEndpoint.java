package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.logging.LogFetcher;
import org.apache.hadoop.ozone.recon.logging.LogFetcherImpl;
import org.apache.hadoop.ozone.recon.logging.LogFileEmptyException;
import org.apache.hadoop.ozone.recon.logging.LogModels.LoggerResponse;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;

import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_LOG_OFFSET;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_LOG_LINES;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_RECON_LOG_LINES;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_LOG_DIRECTION;
import static org.apache.hadoop.ozone.recon.ReconConstants.DEFAULT_RECON_LOG_DIRECTION;


@Path("/log")
@Produces(MediaType.APPLICATION_JSON)
public class LogEndpoint {

  String logDir;
  String logFile;
  String RECON_LOG_FILE_LOC;

  private final LogFetcherImpl logFetcher;
  public LogEndpoint() {
    logDir = System.getProperty("hadoop.log.dir");
    logFile = "ozone-recon.log";
    RECON_LOG_FILE_LOC = logDir + "/" + logFile;
    logFetcher = new LogFetcherImpl();
  }

  @GET
  @Path("/read")
  public Response getLogLines() throws IOException{
    LoggerResponse.Builder respBuilder;
    LogFetcherImpl logFetcher = new LogFetcherImpl();
    try {
      logFetcher.initializeReader(RECON_LOG_FILE_LOC);
      respBuilder = logFetcher.getLogs(100);
    } catch (ParseException pe) {
      return Response.serverError()
        .entity("Unable to parse timestamp for log: \n" + Arrays.toString(pe.getStackTrace()))
        .build();
    } catch (IOException ie) {
      return Response.serverError()
        .entity("Unable to open log file: \n" + Arrays.toString(ie.getStackTrace()))
        .build();
    } catch (NullPointerException npe) {
      return Response.serverError()
        .entity("NullPointerException: \n" + Arrays.toString(npe.getStackTrace()))
        .build();
    } catch (LogFileEmptyException e){
      return Response.status(425)
        .entity(e.getMessage())
        .build();
    }
    finally {
      logFetcher.close();
    }
    return Response.ok(
      respBuilder.setStatus(ResponseStatus.OK).build()
    ).build();
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
    @QueryParam(RECON_LOG_OFFSET)
      int offset,
    @DefaultValue(DEFAULT_RECON_LOG_LINES) @QueryParam(RECON_LOG_LINES)
      int lines,
    @DefaultValue(DEFAULT_RECON_LOG_DIRECTION) @QueryParam(RECON_LOG_DIRECTION)
    LogFetcher.Direction direction
  ) throws IOException{
    LoggerResponse.Builder respBuilder;
    try {
      logFetcher.initializeReader(RECON_LOG_FILE_LOC);
      respBuilder = logFetcher.getLogs(offset, direction, lines);
    } catch (ParseException pe) {
      return Response.serverError()
        .entity("Unable to parse timestamp for log: \n" + Arrays.toString(pe.getStackTrace()))
        .build();
    } catch (FileNotFoundException fe) {
      return Response.serverError()
        .entity("Unable to find log file: \n" + Arrays.toString(fe.getStackTrace()))
        .build();
    } catch (NullPointerException npe) {
      return Response.serverError()
        .entity("NullPointerException: \n" + Arrays.toString(npe.getStackTrace()))
        .build();
    } catch (LogFileEmptyException e){
      return Response.status(425)
        .entity(e.getMessage())
        .build();
    } finally {
      logFetcher.close();
    }

    return Response.ok(
      respBuilder.setStatus(ResponseStatus.OK).build()
    ).build();
  }
}

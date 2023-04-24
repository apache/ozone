/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.ozone.recon.solr;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.inject.Inject;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.api.handlers.EntityHandler;
import org.apache.hadoop.ozone.recon.api.types.AuditLogFacetsResources;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityMetaData;
import org.apache.hadoop.ozone.recon.api.types.EntityReadAccessHeatMapResponse;
import org.apache.hadoop.ozone.recon.api.types.LastXUnit;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.http.HttpRequestWrapper;
import org.apache.hadoop.ozone.recon.http.ReconHttpClient;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_SOLR_TIMEZONE_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_SOLR_ADDRESS_KEY;

/**
 * This class is general utility class for handling
 * Solr query functions.
 */
public class SolrUtil {
  private static final Logger LOG =
      LoggerFactory.getLogger(SolrUtil.class);
  public static final String DEFAULT_TIMEZONE_VALUE = "UTC";

  private OzoneConfiguration ozoneConfiguration;
  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private final ReconOMMetadataManager omMetadataManager;
  private final OzoneStorageContainerManager reconSCM;
  private AtomicReference<EntityReadAccessHeatMapResponse>
      entityReadAccessHeatMapRespRef;
  private SimpleDateFormat dateFormat = new SimpleDateFormat(
      "yyyy-MM-dd'T'HH:mm:ss'Z'");
  private final String timeZone;

  @Inject
  public SolrUtil(ReconNamespaceSummaryManager
                      namespaceSummaryManager,
                  ReconOMMetadataManager omMetadataManager,
                  OzoneStorageContainerManager reconSCM,
                  OzoneConfiguration ozoneConfiguration) {
    this.reconNamespaceSummaryManager = namespaceSummaryManager;
    this.omMetadataManager = omMetadataManager;
    this.reconSCM = reconSCM;
    this.entityReadAccessHeatMapRespRef = new AtomicReference<>(
        new EntityReadAccessHeatMapResponse());
    this.ozoneConfiguration = ozoneConfiguration;
    this.timeZone = this.ozoneConfiguration.get(OZONE_RECON_SOLR_TIMEZONE_KEY,
        DEFAULT_TIMEZONE_VALUE);
    if (timeZone != null) {
      LOG.info("Setting timezone to " + timeZone);
      try {
        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
      } catch (Throwable t) {
        LOG.error("Error setting timezone. TimeZone = " + timeZone);
      }
    }
  }

  private void addBucketData(
      EntityReadAccessHeatMapResponse volumeEntity, String[] split,
      int readAccessCount, long keySize) {
    List<EntityReadAccessHeatMapResponse> children =
        volumeEntity.getChildren();
    EntityReadAccessHeatMapResponse bucketEntity = null;
    List<EntityReadAccessHeatMapResponse> bucketList =
        children.stream().filter(entity -> entity.getLabel().
            equalsIgnoreCase(split[1])).collect(Collectors.toList());
    if (bucketList.size() > 0) {
      bucketEntity = bucketList.get(0);
    }
    if (children.contains(bucketEntity)) {
      addPrefixPathInfoToBucket(split, bucketEntity, readAccessCount, keySize);
    } else {
      addBucketAndPrefixPath(split, volumeEntity, readAccessCount, keySize);
    }
  }

  private void addVolumeData(
      EntityReadAccessHeatMapResponse rootEntity,
      String[] split, int readAccessCount, long keySize) {
    List<EntityReadAccessHeatMapResponse> children =
        rootEntity.getChildren();
    EntityReadAccessHeatMapResponse volumeInfo =
        new EntityReadAccessHeatMapResponse();
    volumeInfo.setLabel(split[0]);
    children.add(volumeInfo);
    addBucketAndPrefixPath(split, volumeInfo, readAccessCount, keySize);
  }

  private void updateVolumeSize(
      EntityReadAccessHeatMapResponse volumeInfo) {
    List<EntityReadAccessHeatMapResponse> children =
        volumeInfo.getChildren();
    children.stream().forEach(bucket -> {
      volumeInfo.setSize(volumeInfo.getSize() + bucket.getSize());
      updateBucketLevelMinMaxAccessCount(bucket);
      updateBucketAccessRatio(bucket);
    });
  }

  private void updateBucketAccessRatio(EntityReadAccessHeatMapResponse bucket) {
    long delta = bucket.getMaxAccessCount() - bucket.getMinAccessCount();
    List<EntityReadAccessHeatMapResponse> children =
        bucket.getChildren();
    children.stream().forEach(path -> {
      path.setColor(1.000);
      if (delta > 0) {
        double truncatedValue = truncate(
            ((double) path.getAccessCount() /
                (double) bucket.getMaxAccessCount()), 3);
        path.setColor(truncatedValue);
      }
    });
  }

  private static double truncate(double value, int decimalPlaces) {
    if (decimalPlaces < 0) {
      throw new IllegalArgumentException();
    }
    value = value * Math.pow(10, decimalPlaces);
    value = Math.floor(value);
    value = value / Math.pow(10, decimalPlaces);
    return value;
  }

  private void updateRootEntitySize(
      EntityReadAccessHeatMapResponse rootEntity) {
    List<EntityReadAccessHeatMapResponse> children =
        rootEntity.getChildren();
    children.stream().forEach(volume -> {
      updateVolumeSize(volume);
      rootEntity.setSize(rootEntity.getSize() + volume.getSize());
    });
  }

  private void addBucketAndPrefixPath(
      String[] split, EntityReadAccessHeatMapResponse volumeEntity,
      long readAccessCount, long keySize) {
    List<EntityReadAccessHeatMapResponse> bucketEntities =
        volumeEntity.getChildren();
    EntityReadAccessHeatMapResponse bucket =
        new EntityReadAccessHeatMapResponse();
    bucket.setLabel(split[1]);
    bucketEntities.add(bucket);
    bucket.setMinAccessCount(readAccessCount);
    addPrefixPathInfoToBucket(split, bucket, readAccessCount, keySize);
  }

  private void addPrefixPathInfoToBucket(
      String[] split, EntityReadAccessHeatMapResponse bucket,
      long readAccessCount, long keySize) {
    List<EntityReadAccessHeatMapResponse> prefixes = bucket.getChildren();
    updateBucketSize(bucket, keySize);
    String path = Arrays.stream(split)
        .skip(2).collect(Collectors.joining("/"));
    EntityReadAccessHeatMapResponse prefixPathInfo =
        new EntityReadAccessHeatMapResponse();
    prefixPathInfo.setLabel(path);
    prefixPathInfo.setAccessCount(readAccessCount);
    prefixPathInfo.setSize(keySize);
    prefixes.add(prefixPathInfo);
    // This is done for specific ask by UI treemap to render and provide
    // varying color shades based on varying ranges of access count.
    updateRootLevelMinMaxAccessCount(readAccessCount);
  }

  private void updateBucketLevelMinMaxAccessCount(
      EntityReadAccessHeatMapResponse bucket) {
    List<EntityReadAccessHeatMapResponse> children =
        bucket.getChildren();
    if (children.size() > 0) {
      bucket.setMinAccessCount(Long.MAX_VALUE);
    }
    children.stream().forEach(path -> {
      long readAccessCount = path.getAccessCount();
      bucket.setMinAccessCount(
          path.getAccessCount() < bucket.getMinAccessCount() ? readAccessCount :
              bucket.getMinAccessCount());
      bucket.setMaxAccessCount(
          readAccessCount > bucket.getMaxAccessCount() ? readAccessCount :
              bucket.getMaxAccessCount());
    });
  }

  private void updateRootLevelMinMaxAccessCount(long readAccessCount) {
    EntityReadAccessHeatMapResponse rootEntity =
        this.entityReadAccessHeatMapRespRef.get();
    rootEntity.setMinAccessCount(
        readAccessCount < rootEntity.getMinAccessCount() ? readAccessCount :
            rootEntity.getMinAccessCount());
    rootEntity.setMaxAccessCount(
        readAccessCount > rootEntity.getMaxAccessCount() ? readAccessCount :
            rootEntity.getMaxAccessCount());
  }

  private void updateBucketSize(EntityReadAccessHeatMapResponse bucket,
                                       long keySize) {
    bucket.setSize(bucket.getSize() + keySize);
  }

  public void queryLogs(String path, String entityType, String startDate,
                        ReconHttpClient reconHttpClient) {
    try {
      SecurityUtil.doAsCurrentUser((PrivilegedExceptionAction<Void>) () -> {
        InetSocketAddress solrAddress =
            HddsUtils.getSolrAddress(ozoneConfiguration);
        if (null == solrAddress) {
          throw new ConfigurationException(String.format("For heatmap " +
                  "feature Solr host and port configuration must be provided " +
                  "for config key %s. Example format -> <Host>:<Port>",
              OZONE_SOLR_ADDRESS_KEY));
        }
        List<NameValuePair> urlParameters = new ArrayList<>();
        validateAndAddSolrReqParam(escapeQueryParamVal(path),
            "resource", urlParameters);
        validateAndAddSolrReqParam(entityType, "resType", urlParameters);
        validateStartDate(startDate, urlParameters);
        final String solrAuditResp =
            reconHttpClient.sendRequest(
                prepareHttpRequest(urlParameters, solrAddress,
                    "/solr/ranger_audits/query"));
        LOG.info("Solr Response: {}", solrAuditResp);
        JsonElement jsonElement = JsonParser.parseString(solrAuditResp);
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        JsonElement facets = jsonObject.get("facets");
        JsonElement resources = facets.getAsJsonObject().get("resources");
        JsonObject facetsBucketsObject = new JsonObject();
        if (null != resources) {
          facetsBucketsObject = resources.getAsJsonObject();
        }
        ObjectMapper objectMapper = new ObjectMapper();

        AuditLogFacetsResources auditLogFacetsResources =
            objectMapper.readValue(
                facetsBucketsObject.toString(), AuditLogFacetsResources.class);
        EntityMetaData[] entities = auditLogFacetsResources.getBuckets();
        if (null != entities && !(ArrayUtils.isEmpty(entities))) {
          generateHeatMap(entities);
        }
        return null;
      });
    } catch (JsonProcessingException e) {
      LOG.error("Solr Query Output Processing Error: {} ", e);
    } catch (IOException e) {
      LOG.error("Error while generating the access heatmap: {} ", e);
    }
  }

  private String escapeQueryParamVal(String path) {
    StringBuilder sb = new StringBuilder();
    if (!StringUtils.isEmpty(path)) {
      sb.append("*");
      sb.append(ClientUtils.escapeQueryChars(path));
      sb.append("*");
    }
    return sb.toString();
  }

  private void validateStartDate(String startDate,
                                 List<NameValuePair> urlParameters) {
    if (!StringUtils.isEmpty(startDate)) {
      ZonedDateTime lastXUnitsOfZonedDateTime = null;
      startDate = validateStartDate(startDate);
      if (null != LastXUnit.getType(startDate)) {
        lastXUnitsOfZonedDateTime =
            lastXUnitsOfTime(LastXUnit.getType(startDate));
      } else {
        lastXUnitsOfZonedDateTime =
            epochMilliSToZDT(startDate);
      }
      urlParameters.add(new BasicNameValuePair("fq",
          setDateRange("evtTime",
              Date.from(lastXUnitsOfZonedDateTime.toInstant()), null)));
    }
  }

  private void validateAndAddSolrReqParam(
      String paramVal, String paramName,
      List<NameValuePair> urlParameters) {
    if (!StringUtils.isEmpty(paramVal)) {
      StringBuilder sb = new StringBuilder(paramName);
      sb.append(":");
      sb.append(paramVal);
      urlParameters.add(new BasicNameValuePair("fq", sb.toString()));
    }
  }

  private HttpRequestWrapper prepareHttpRequest(
      List<NameValuePair> urlParameters, InetSocketAddress solrAddress,
      String uri) {
    // add request parameter, form parameters
    urlParameters.add(new BasicNameValuePair("q", "*:*"));
    urlParameters.add(new BasicNameValuePair("wt", "json"));
    urlParameters.add(new BasicNameValuePair("fl",
        "access, agent, repo, resource, resType, event_count"));
    urlParameters.add(new BasicNameValuePair("fq", "access:read"));
    urlParameters.add(new BasicNameValuePair("fq", "repo:cm_ozone"));

    urlParameters.add(new BasicNameValuePair("sort", "event_count desc"));
    urlParameters.add(new BasicNameValuePair("start", "0"));
    urlParameters.add(new BasicNameValuePair("rows", "0"));

    urlParameters.add(new BasicNameValuePair("json.facet", "{\n" +
        "    resources:{\n" +
        "      type : terms,\n" +
        "      field : resource,\n" +
        "      sort : \"read_access_count desc\",\n" +
        "      limit : 100,\n" +
        "      facet:{\n" +
        "        read_access_count : \"sum(event_count)\"\n" +
        "      }\n" +
        "    }\n" +
        "  }"));
    HttpRequestWrapper requestWrapper =
        new HttpRequestWrapper(solrAddress.getHostName(),
            solrAddress.getPort(), uri,
            urlParameters, HttpRequestWrapper.HttpReqType.POST);
    return requestWrapper;
  }

  private void generateHeatMap(EntityMetaData[] entities) {
    EntityReadAccessHeatMapResponse rootEntity =
        entityReadAccessHeatMapRespRef.get();
    rootEntity.setMinAccessCount(entities[0].getReadAccessCount());
    rootEntity.setLabel("root");
    List<EntityReadAccessHeatMapResponse> children =
        rootEntity.getChildren();
    Arrays.stream(entities).forEach(entityMetaData -> {
      String path = entityMetaData.getVal();
      String[] split = path.split("/");
      if (split.length == 0) {
        return;
      }
      long keySize = 0;
      try {
        keySize = getEntitySize(path);
      } catch (IOException e) {
        LOG.error("IOException while getting key size for key : " +
            "{} - {}", path, e);
      }
      EntityReadAccessHeatMapResponse volumeEntity = null;
      List<EntityReadAccessHeatMapResponse> volumeList =
          children.stream().filter(entity -> entity.getLabel().
              equalsIgnoreCase(split[0])).collect(Collectors.toList());
      if (volumeList.size() > 0) {
        volumeEntity = volumeList.get(0);
      }
      if (null != volumeEntity) {
        addBucketData(volumeEntity, split, entityMetaData.getReadAccessCount(),
            keySize);
      } else {
        addVolumeData(rootEntity, split,
            entityMetaData.getReadAccessCount(), keySize);
      }
    });
    updateRootEntitySize(rootEntity);
  }

  private long getEntitySize(String path) throws IOException {
    long entitySize = 0;
    LOG.info("Getting entity size for {}: ", path);
    EntityHandler entityHandler =
        EntityHandler.getEntityHandler(reconNamespaceSummaryManager,
            omMetadataManager, reconSCM, path);
    if (null != entityHandler) {
      DUResponse duResponse = entityHandler.getDuResponse(false, false);
      if (null != duResponse && duResponse.getStatus() == ResponseStatus.OK) {
        return duResponse.getSize();
      }
    }
    // returning some default value due to some issue
    return 256L;
  }

  public EntityReadAccessHeatMapResponse getEntityReadAccessHeatMapResponse() {
    return entityReadAccessHeatMapRespRef.get();
  }

  public String setDateRange(String fieldName, Date fromDate, Date toDate) {
    String fromStr = "*";
    String toStr = "NOW";
    if (fromDate != null) {
      fromStr = dateFormat.format(fromDate);
    }
    if (toDate != null) {
      toStr = dateFormat.format(toDate);
    }
    return fieldName + ":[" + fromStr + " TO " + toStr + "]";
  }

  private String validateStartDate(String startDate) {
    if (null != LastXUnit.getType(startDate)) {
      return startDate;
    }
    long epochMilliSeconds = 0L;
    try {
      epochMilliSeconds = Long.parseLong(startDate);
    } catch (NumberFormatException nfe) {
      LOG.error(
          "Unsupported Last X units of time : {}, falling back to default 24H",
          startDate);
      return LastXUnit.TWENTY_FOUR_HOUR.getValue();
    }
    if (epochMilliSeconds > Instant.now().toEpochMilli()) {
      LOG.error(
          "Unsupported Last X units of time : {}, falling back to default 24H",
          startDate);
      return LastXUnit.TWENTY_FOUR_HOUR.getValue();
    }
    return startDate;
  }

  private ZonedDateTime epochMilliSToZDT(String epochMilliSeconds) {
    Long lEpochMilliSeconds = Long.parseLong(epochMilliSeconds);
    return ZonedDateTime.ofInstant(
        Instant.ofEpochMilli(lEpochMilliSeconds),
        TimeZone.getTimeZone(timeZone).toZoneId());
  }

  private ZonedDateTime lastXUnitsOfTime(LastXUnit lastXUnit) {
    ZonedDateTime zonedDateTime = null;
    switch (lastXUnit) {
    case TWENTY_FOUR_HOUR:
      zonedDateTime = Instant.now().atZone(
              TimeZone.getTimeZone(timeZone).toZoneId())
          .minus(24, ChronoUnit.HOURS);
      break;
    case SEVEN_DAYS:
      zonedDateTime = Instant.now().atZone(
          TimeZone.getTimeZone(timeZone).toZoneId()).minus(7, ChronoUnit.DAYS);
      break;
    case NINETY_DAYS:
      zonedDateTime = Instant.now().atZone(
              TimeZone.getTimeZone(timeZone).toZoneId())
          .minus(90, ChronoUnit.DAYS);
      break;
    default:
      throw new IllegalArgumentException(
          "Unsupported Last X units of time : " + lastXUnit);
    }
    return zonedDateTime;
  }
}

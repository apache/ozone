package org.apache.hadoop.ozone.om.eventlistener;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;

import org.apache.hadoop.ozone.om.OmMetadataReader;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An implementation of NotificationCheckpointStrategy which loads/saves
 * the the last known notification sent by a event notification plugin
 * to the Ozone filesystem itself.
 *
 * This allows another OM to pick up from the appropriate place in the
 * event of a leadership change.
 */
public class OzoneFileCheckpointStrategy implements NotificationCheckpointStrategy {

  public static final Logger LOG = LoggerFactory.getLogger(OzoneFileCheckpointStrategy.class);

  private static final String VOLUME = "notifications";
  private static final String BUCKET = "checkpoint";
  private static final String KEY = "test";

  // TODO: this seems like the wrong thing to do but in a docker
  // environment the createKey request chokes unless this is set to some
  // value
  private static final UserInfo USER_INFO = UserInfo.newBuilder()
      .setUserName("user")
      .setHostName("localhost")
      .setRemoteAddress("127.0.0.1")
      .build();

  private final AtomicLong callId = new AtomicLong(0);
  private final ClientId clientId = ClientId.randomId();
  private final OzoneManager ozoneManager;
  private final OmMetadataReader omMetadataReader;
  private final AtomicLong saveCount = new AtomicLong(0);


  public OzoneFileCheckpointStrategy(OzoneManager ozoneManager, final OmMetadataReader omMetadataReader) {
    this.ozoneManager = ozoneManager;
    this.omMetadataReader = omMetadataReader;
  }

  public String load() throws IOException {
    try {

      OmKeyArgs omKeyArgs = new OmKeyArgs.Builder().setVolumeName(VOLUME).setBucketName(BUCKET).setKeyName(KEY).build();
      String result =
          omMetadataReader.getFileStatus(omKeyArgs).getKeyInfo().getMetadata().get("notification-checkpoint");
      return result;
    } catch (IOException ex) {
      LOG.info("Error loading notification checkpoint {}", ex);
      return null;
    }
  }

  public void save(String val) throws IOException {
    long previousSaveCount = saveCount.getAndIncrement();
    if (previousSaveCount == 0 || previousSaveCount % 100 == 0) {
      Map<String, String> metadata = new HashMap<>();
      metadata.put("notification-checkpoint", val);
      writeCheckpointFile(VOLUME, BUCKET, KEY, metadata);
      LOG.info("Persisted notification-checkpoint {} to /{}/{}/{}", val, VOLUME, BUCKET, KEY);
    }
  }

  private void writeCheckpointFile(String volumeName,
                                   String bucketName,
                                   String keyName,
                                   Map<String, String> metadata) throws IOException {

    CreateKeyResponse createResponse = submitRequest(
        buildCreateKeyRequest(volumeName, bucketName, keyName, metadata)).getCreateKeyResponse();

    // NOTE: we used the sessionId from the createKey response in the
    // commitKey request (to indicate which session we are committing)
    long sessionId = createResponse.getID();
    submitRequest(buildCommitKeyRequest(volumeName, bucketName, keyName, metadata, sessionId));
  }

  private OMRequest buildCreateKeyRequest(String volumeName,
                                          String bucketName,
                                          String keyName,
                                          Map<String, String> metadata) {

    CreateKeyRequest createKeyRequest = CreateKeyRequest.newBuilder()
        .setKeyArgs(createKeyArgs(volumeName, bucketName, keyName, metadata))
        .build();

    return OMRequest.newBuilder()
        .setCmdType(Type.CreateKey)
        .setClientId(clientId.toString())
        .setCreateKeyRequest(createKeyRequest)
        .setUserInfo(USER_INFO)
        .build();
  }

  private OMRequest buildCommitKeyRequest(String volumeName,
                                          String bucketName,
                                          String keyName,
                                          Map<String, String> metadata,
                                          long sessionId) {

    CommitKeyRequest commitKeyRequest = CommitKeyRequest.newBuilder()
        .setKeyArgs(createKeyArgs(volumeName, bucketName, keyName, metadata))
        .setClientID(sessionId)
        .build();

    return OMRequest.newBuilder()
        .setCmdType(Type.CommitKey)
        .setClientId(clientId.toString())
        .setCommitKeyRequest(commitKeyRequest)
        .build();
  }

  private static KeyArgs.Builder createKeyArgs(String volumeName,
                                               String bucketName,
                                               String keyName,
                                               Map<String, String> metadata) {

    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName);

    // Include metadata, if provided
    if (metadata != null && !metadata.isEmpty()) {
      metadata.forEach((key, value) -> keyArgs.addMetadata(HddsProtos.KeyValue.newBuilder()
          .setKey(key)
          .setValue(value)
          .build()));
    }

    return keyArgs;
  }

  private OMResponse submitRequest(OMRequest omRequest) {
    try {
      return OzoneManagerRatisUtils.submitRequest(ozoneManager, omRequest, clientId, callId.incrementAndGet());
    } catch (ServiceException e) {
      LOG.error("Open key " + omRequest.getCmdType() +
          " request failed. Will retry at next run.", e);
    }
    return null;
  }

}

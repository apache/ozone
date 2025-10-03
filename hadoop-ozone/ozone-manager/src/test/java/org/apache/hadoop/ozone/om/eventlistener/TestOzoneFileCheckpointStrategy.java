package org.apache.hadoop.ozone.om.eventlistener;


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.ozone.om.OmMetadataReader;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.ratis.protocol.ClientId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Tests {@link OzoneFileCheckpointStrategy}.
 */
@ExtendWith(MockitoExtension.class)
public class TestOzoneFileCheckpointStrategy {

  @Mock
  OzoneManager mockOzoneManager;
  @Mock
  OmMetadataReader mockOmMetadataReader;
  OzoneManagerRatisServer mockOzoneManagerRatisServer;
  @Mock
  OzoneManagerProtocolProtos.OMResponse mockOmResponse;
  @Mock
  OzoneManagerProtocolProtos.CreateKeyResponse mockOmCreateResponse;
  OzoneFileStatus fileStatus;
  OzoneFileCheckpointStrategy ozoneFileCheckpointStrategy;

  @BeforeEach
  public void setup() {
    ozoneFileCheckpointStrategy = new OzoneFileCheckpointStrategy(mockOzoneManager, mockOmMetadataReader);
  }

  @Test
  public void testSaveStrategy() throws IOException, ServiceException {

    when(mockOmCreateResponse.getID()).thenReturn(123L);
    when(mockOmResponse.getCreateKeyResponse()).thenReturn(mockOmCreateResponse);
    try (MockedStatic<OzoneManagerRatisUtils> utils = mockStatic(OzoneManagerRatisUtils.class)) {

      utils.when(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class), any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class))).thenReturn(mockOmResponse);

      //Check its saved on first iteration
      ozoneFileCheckpointStrategy.save("00000000000000000001");
      utils.verify(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class), any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class)), Mockito.times(2));
      //But not on second
      ozoneFileCheckpointStrategy.save("0000000000000000002");
      utils.verify(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class), any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class)), Mockito.times(2));

      for (int i = 0; i <=100; i++) {
        String val = String.format("%020d", i);
        ozoneFileCheckpointStrategy.save(val);
      }

      //Check submit has only ran twice(4 times in total)
      utils.verify(() -> OzoneManagerRatisUtils.submitRequest(any(OzoneManager.class), any(OzoneManagerProtocolProtos.OMRequest.class),
          any(ClientId.class), any(Long.class)), Mockito.times(4));
    }
  }

  @Test
  public void testLoadStrategyWhenMetadataNotSet() throws IOException {
    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder().build();
    fileStatus = new OzoneFileStatus(omKeyInfo, 1L, false);
    when(mockOmMetadataReader.getFileStatus(any())).thenReturn(fileStatus);
    Assertions.assertEquals(null, ozoneFileCheckpointStrategy.load());
  }

  @Test
  public void testLoadStrategyWhenFileDoesNotExist() throws IOException {
    when(mockOmMetadataReader.getFileStatus(any())).thenThrow(FileNotFoundException.class);
    Assertions.assertEquals(null, ozoneFileCheckpointStrategy.load());
  }

  @Test
  public void testLoadStrategyWithValidMetaData() throws IOException {
    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder().addMetadata("notification-checkpoint", "00000000000000000017").build();
    fileStatus = new OzoneFileStatus(omKeyInfo, 1L, false);
    when(mockOmMetadataReader.getFileStatus(any())).thenReturn(fileStatus);
    Assertions.assertEquals("00000000000000000017", ozoneFileCheckpointStrategy.load());
  }
}

package org.apache.hadoop.ozone.om.protocolPB;


import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import java.io.IOException;
import java.util.Collections;

import static org.mockito.Mockito.verify;

class TestOzoneManagerProtocolClientSideTranslatorPB {

  @Mock
  private OmTransport omTransport;

  private OzoneManagerProtocolClientSideTranslatorPB pb = new OzoneManagerProtocolClientSideTranslatorPB(
      omTransport, "test-client-id");

  @Test
  void testStartQuotaRepair() throws IOException {
    ArgumentCaptor<OMRequest> captor = ArgumentCaptor.forClass(OMRequest.class);
    
    pb.startQuotaRepair(Collections.emptyList());

    verify(omTransport.submitRequest(captor.capture()));
    
    OMRequest 
  }

}

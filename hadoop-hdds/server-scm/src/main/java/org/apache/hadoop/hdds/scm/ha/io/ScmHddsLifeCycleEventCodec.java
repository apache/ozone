package org.apache.hadoop.hdds.scm.ha.io;

import com.google.common.primitives.Ints;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;

/**
 * Encodes/decodes {@link HddsProtos.LifeCycleEvent}.
 */
public class ScmHddsLifeCycleEventCodec
    implements ScmCodec<HddsProtos.LifeCycleEvent> {

  @Override
  public ByteString serialize(HddsProtos.LifeCycleEvent object) {
    return UnsafeByteOperations.unsafeWrap(
        Ints.toByteArray(object.getNumber()));
  }

  @Override
  public HddsProtos.LifeCycleEvent deserialize(Class<?> type, ByteString value) {
    int n = Ints.fromByteArray(value.toByteArray());
    HddsProtos.LifeCycleEvent event = HddsProtos.LifeCycleEvent.forNumber(n);
    if (event == null) {
      throw new IllegalArgumentException(
          "Unknown LifeCycleEvent enum value: " + n);
    }
    return event;
  }
}

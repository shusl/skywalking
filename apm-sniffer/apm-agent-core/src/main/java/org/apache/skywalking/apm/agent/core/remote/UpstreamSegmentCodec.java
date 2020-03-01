package org.apache.skywalking.apm.agent.core.remote;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.skywalking.apm.commons.datacarrier.buffer.QueueCodec;
import org.apache.skywalking.apm.network.language.agent.UpstreamSegment;

public class UpstreamSegmentCodec implements QueueCodec<UpstreamSegment> {
	@Override
	public UpstreamSegment decode(byte[] bytes) {
		UpstreamSegment segment = null;
		try {
			segment = UpstreamSegment.parseFrom(bytes);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();  //e:
		}
		return segment;
	}

	@Override
	public byte[] encode(UpstreamSegment obj) {
		return obj.toByteArray();
	}
}

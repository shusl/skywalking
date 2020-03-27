package org.apache.skywalking.apm.agent.core.remote;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.skywalking.apm.commons.datacarrier.buffer.QueueCodec;
import org.apache.skywalking.apm.network.language.agent.UpstreamSegment;
import org.apache.skywalking.apm.network.language.agent.UpstreamSegmentList;

/**
 * @Author: shushenglin
 * @Date: 2020/3/20 19:35
 */
public class UpstreamSegmentsCodec implements QueueCodec<UpstreamSegmentList> {
	@Override
	public UpstreamSegmentList decode(byte[] bytes) {
		UpstreamSegmentList upstreamSegmentList = null;
		try {
			upstreamSegmentList = UpstreamSegmentList.parseFrom(bytes);
		} catch (InvalidProtocolBufferException e) {
			try {
				UpstreamSegment upstreamSegment = UpstreamSegment.parseFrom(bytes);
				UpstreamSegmentList.Builder builder = UpstreamSegmentList.newBuilder();
				builder.addSegments(upstreamSegment);
				upstreamSegmentList = builder.build();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		return upstreamSegmentList;
	}

	@Override
	public byte[] encode(UpstreamSegmentList obj) {
		return obj.toByteArray();
	}
}

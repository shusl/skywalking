package org.apache.skywalking.apm.commons.datacarrier.buffer;

import java.util.concurrent.ScheduledExecutorService;

/**
 * channel with FQueue
 * @param <T>
 */
public class FQueueChannels<T> extends Channels<T> {
	private FQueueBuffer<T> fQueueBuffer;

	public FQueueChannels(QueueCodec<T> codec, ScheduledExecutorService scheduler, int logSize) {
		fQueueBuffer = new FQueueBuffer<T>(codec, scheduler, logSize);
	}

	@Override
	public boolean save(T data) {
		return fQueueBuffer.save(data);
	}

	@Override
	public int getChannelSize() {
		return 1;
	}

	@Override
	public long size() {
		// 不限制容量
		return Integer.MAX_VALUE;
	}

	@Override
	public QueueBuffer getBuffer(int index) {
		return fQueueBuffer;
	}

	@Override
	public void close() {
		fQueueBuffer.close();
	}
}

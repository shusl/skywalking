package org.apache.skywalking.apm.commons.datacarrier.buffer;

import com.google.code.fqueue.FQueue;
import org.apache.skywalking.apm.commons.datacarrier.EnvUtil;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * QueueBuffer implement by FQueue
 * @param <T>
 */
public class FQueueBuffer<T> implements QueueBuffer<T> {
	private FQueue fQueue;
	private QueueCodec<T> codec;
	private int maxBatchSize = 2000;

	public FQueueBuffer(QueueCodec<T> codec, ScheduledExecutorService executorService, int logSize) {
		maxBatchSize = getIntProperty("FQueue.batch.size", 2000);
		String dbPath = System.getProperty("FQueue.db.path", "./db");
		this.codec = codec;
		try {
			fQueue = new FQueue(dbPath, logSize, executorService);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public FQueueBuffer(QueueCodec<T> codec, ScheduledExecutorService executorService) {
		this(codec, executorService, getIntProperty("FQueue.log.size", 1024 * 1024 * 30));
	}

	public static int getIntProperty(String key, int defaultValue) {
		int retVal = defaultValue;
		String val = System.getProperty(key);
		if (val != null) {
			try {
				retVal = Integer.parseInt(val);
			} catch (NumberFormatException ignore) {
			}
		}
		return retVal;
	}

	@Override
	public boolean save(T data) {
		if (fQueue == null) {
			return false;
		}
		byte[] bytes = codec.encode(data);
		if (bytes == null || bytes.length == 0) {
			return false;
		}
		return fQueue.offer(bytes);
	}

	@Override
	public void setStrategy(BufferStrategy strategy) {

	}

	@Override
	public void obtain(List<T> consumeList) {
		if (fQueue == null) {
			return;
		}
		maxBatchSize = EnvUtil.getInt("FQueue.batch.size", maxBatchSize);
		for (int i =0; i< maxBatchSize; ++i) {
			byte[] bytes = fQueue.poll();
			if (bytes == null) {
				break;
			}
			T obj = codec.decode(bytes);
			consumeList.add(obj);
		}
	}

	@Override
	public int getBufferSize() {
		if (fQueue == null) {
			return 0;
		}
		return fQueue.size();
	}

	public void close(){
		if (fQueue != null) {
			fQueue.close();
			fQueue = null;
		}
	}
}

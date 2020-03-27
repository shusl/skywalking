package org.apache.skywalking.apm.commons.datacarrier.buffer;

/**
 * FQueue codec
 * @param <T>
 */
public interface QueueCodec<T> {
	/**
	 * decode bytes from fqueue
	 * @param bytes
	 * @return
	 */
	T decode(byte[] bytes);

	/**
	 * encode data as bytes
	 * @param obj
	 * @return
	 */
	byte[] encode(T obj);
}

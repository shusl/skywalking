package com.google.code.fqueue;

import com.google.code.fqueue.exception.FileEOFException;
import com.google.code.fqueue.exception.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Author: shushenglin
 * Date:   2017/12/11 17:05
 */
public class FBlockingQueue extends AbstractQueue<byte[]> implements BlockingQueue<byte[]> {

	public static final int LOG_FILE_SIZE = 1024 * 1024 * 300;

	private FSQueue fsQueue = null;
	private static final Logger log = LoggerFactory.getLogger(FBlockingQueue.class);
	private ReentrantLock lock = new ReentrantLock();
	private Condition notEmpty = lock.newCondition();
	
	public FBlockingQueue(String path, ScheduledExecutorService executorService) throws Exception {
		fsQueue = new FSQueue(path, LOG_FILE_SIZE, executorService);
	}

	public FBlockingQueue(String path, int logSize, ScheduledExecutorService executorService) throws Exception {
		fsQueue = new FSQueue(path, logSize, executorService);
	}

	@Override
	public Iterator<byte[]> iterator() {
		throw new UnsupportedOperationException("iterator Unsupported now");
	}

	@Override
	public int size() {
		return fsQueue.getQueueSize();
	}

	@Override
	public boolean offer(byte[] e) {
		checkNotNull(e);
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			return enqueue(e);
		} finally {
			lock.unlock();
		}
	}

	private boolean enqueue(byte[] e) {
		try {
			fsQueue.add(e);
			notEmpty.signal();
			return true;
		} catch (IOException ex) {
			log.error("enqueue element error", ex);
		} catch (FileFormatException ex) {
			log.error("enqueue element error", ex);
		}
		return false;
	}

	@Override
	public byte[] peek() {
		try {
			lock.lock();
			return fsQueue.peek();
		} catch (IOException e) {
			log.error(e.getMessage(), e);
			return null;
		} catch (FileFormatException e) {
			log.error(e.getMessage(), e);
			return null;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public byte[] poll() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			return dequeue();
		} finally {
			lock.unlock();
		}
	}
	private byte[] dequeue(){
		try {
			return fsQueue.readNextAndRemove();
		} catch (IOException e) {
			log.error(e.getMessage(), e);
			return null;
		} catch (FileFormatException e) {
			log.error(e.getMessage(), e);
			return null;
		}
	}

	public void close() {
		if (fsQueue != null) {
			fsQueue.close();
		}
	}
	@Override
	public void put(byte[] e) throws InterruptedException {
		checkNotNull(e);
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			enqueue(e);
		} finally {
			lock.unlock();
		}
	}

	/**
	 * remove the first element and return null
	 *
	 * @return always return null
	 */
	public byte[] remove() {
		try {
			lock.lock();
			fsQueue.remove();
		} catch (FileEOFException e) {
			log.error(e.getMessage(), e);
		} finally {
			lock.unlock();
		}
		return null;
	}

	@Override
	public boolean offer(byte[] e, long timeout, TimeUnit unit) throws InterruptedException {
		checkNotNull(e);
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			return enqueue(e);
		} finally {
			lock.unlock();
		}
	}

	public byte[] peekOrWait() throws InterruptedException {
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			while (size() == 0)
				notEmpty.await();
			return fsQueue.peek();
		} catch (IOException e) {
			log.error(e.getMessage(), e);
			return null;
		} catch (FileFormatException e) {
			log.error(e.getMessage(), e);
			return null;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public byte[] take() throws InterruptedException {
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			while (size() == 0)
				notEmpty.await();
			return dequeue();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public byte[] poll(long timeout, TimeUnit unit) throws InterruptedException {
		long nanos = unit.toNanos(timeout);
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			while (size() == 0) {
				if (nanos <= 0)
					return null;
				nanos = notEmpty.awaitNanos(nanos);
			}
			return dequeue();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public int remainingCapacity() {
		return size();
	}

	@Override
	public int drainTo(Collection<? super byte[]> c) {
		return drainTo(c,Integer.MAX_VALUE);
	}

	@Override
	public int drainTo(Collection<? super byte[]> c, int maxElements) {
		checkNotNull(c);
		if (maxElements <= 0)
			return 0;
		final ReentrantLock lock = this.lock;
		lock.lock();
		try{
			int n = Math.min(maxElements, size());
			int i = 0;
			while (i < n) {
				byte[] bytes = dequeue();
				if (bytes != null) {
					c.add(bytes);
				}else {
					break;
				}
				i++;
			}
			return i;
		}finally {
			lock.unlock();
		}
	}

	private static void checkNotNull(Object v) {
		if (v == null)
			throw new NullPointerException();
	}
}

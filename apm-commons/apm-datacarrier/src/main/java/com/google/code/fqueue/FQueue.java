/*
 *  Copyright 2011 sunli [sunli1223@gmail.com][weibo.com@sunli1223]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.google.code.fqueue;

import com.google.code.fqueue.exception.FileEOFException;
import com.google.code.fqueue.exception.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 基于文件系统的持久化队列
 * 
 * @author sunli
 * @date 2010-8-13
 * @version $Id: FQueue.java 2 2011-07-31 12:25:36Z sunli1223@gmail.com $
 */
public class FQueue extends AbstractQueue<byte[]> implements Queue<byte[]>,
		java.io.Serializable {
	private static final long serialVersionUID = -5960741434564940154L;
	private FSQueue fsQueue = null;
	private static final Logger log = LoggerFactory.getLogger(FQueue.class);
	private Lock lock = new ReentrantLock();
	private boolean closed = false;

	public FQueue(String path, ScheduledExecutorService executorService) throws Exception {
		fsQueue = new FSQueue(path, 1024 * 1024 * 300, executorService);
	}

	public FQueue(String path, int logsize, ScheduledExecutorService executorService) throws Exception {
		fsQueue = new FSQueue(path, logsize, executorService);
	}

	@Override
	public Iterator<byte[]> iterator() {
		throw new UnsupportedOperationException("iterator Unsupported now");
	}

	@Override
	public int size() {
		return fsQueue.getQueueSize();
	}

	public boolean offer(byte[] e, int offset, int length) {
		lock.lock();
		try {
			if (closed) {
				return false;
			}
			log.debug("offer data with offset {} length {}", offset, length);
			fsQueue.add(e, offset, length);
			return true;
		} catch (IOException e1) {
			log.error("offer with offset {} length {} error",offset, length, e1);
		} catch (FileFormatException e1) {
			log.error("offer with offset {} length {} error",offset, length, e1);
		} finally {
			lock.unlock();
		}
		return false;
	}

	@Override
	public boolean offer(byte[] e) {
		return offer(e, 0, e.length);
	}

	@Override
	public byte[] peek() {
		lock.lock();
		try {
			if (closed) {
				return null;
			}
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

	/**
	 * remove the first element and return null
	 * @return always return null
	 */
	public byte[] remove(){
		lock.lock();
		try {
			if (closed) {
				return null;
			}
			fsQueue.remove();
		} catch (FileEOFException e) {
			log.error(e.getMessage(), e);
		} finally {
			lock.unlock();
		}
		return null;
	}

	@Override
	public byte[] poll() {
		lock.lock();
		try {
			if (closed) {
				return null;
			}
			return fsQueue.readNextAndRemove();
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

	public void close() {
		lock.lock();
		try {
			if (closed) {
				return;
			}
			closed = true;
			if (fsQueue != null) {
				fsQueue.close();
			}
		}finally {
			lock.unlock();
		}
	}
}

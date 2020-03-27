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
import com.google.code.fqueue.log.FileRunner;
import com.google.code.fqueue.log.LogEntity;
import com.google.code.fqueue.log.LogIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 完成基于文件的先进先出的读写功能
 * 
 * @author sunli
 * @date 2010-8-13
 * @version $Id: FSQueue.java 2 2011-07-31 12:25:36Z sunli1223@gmail.com $
 */
public class FSQueue {
	private static final Logger log = LoggerFactory.getLogger(FSQueue.class);
	public static final String filePrefix = "fqueue";
	private int fileLimitLength = 1024 * 1024 * 100;
	private static final String dbName = "fsqueue.db";
	private static final String fileSeparator = System.getProperty("file.separator");
	private String path = null;
	private ScheduledExecutorService scheduler;
	/**
	 * 文件操作实例
	 */
	private LogIndex db = null;
	private LogEntity writerHandle = null;
	private LogEntity readerHandle = null;
	private FileRunner fileRunner;
	/**
	 * 文件操作位置信息
	 */
	private int readerIndex = -1;
	private int writerIndex = -1;

	public FSQueue(String path, ScheduledExecutorService executorService) throws Exception {
		this(path, 1024 * 1024 * 150, executorService);
	}

	/**
	 * 在指定的目录中，以fileLimitLength为单个数据文件的最大大小限制初始化队列存储
	 * 
	 * @param dir
	 *            队列数据存储的路径
	 * @param fileLimitLength
	 *            单个数据文件的大小，不能超过2G
	 * @throws Exception
	 */
	public FSQueue(String dir, int fileLimitLength, ScheduledExecutorService executorService) throws Exception {
		this.fileLimitLength = fileLimitLength;
		this.scheduler = executorService;
		File fileDir = new File(dir);
		if (fileDir.exists() == false && fileDir.isDirectory() == false) {
			if (fileDir.mkdirs() == false) {
				throw new IOException("create dir error");
			}
		}
		path = fileDir.getAbsolutePath();
		// 打开db
		db = new LogIndex(path + fileSeparator + dbName);
		writerIndex = db.getWriterIndex();
		readerIndex = db.getReaderIndex();
		if (writerIndex > 0 || readerIndex > 0){
			log.info("FSQueue recover from dir {} writer index {} reader index {}", dir, writerIndex, readerIndex);
		}
		String dataDir = path + fileSeparator + filePrefix + "data_";
		fileRunner = new FileRunner(dataDir, fileLimitLength, executorService);
		writerHandle = createLogEntity(dataDir + writerIndex + ".idb", db,
				writerIndex);
		if (readerIndex == writerIndex) {
			readerHandle = writerHandle;
		} else {
			readerHandle = createLogEntity(dataDir + readerIndex + ".idb", db,
					readerIndex);

		}
	}

	/**
	 * 创建或者获取一个数据读写实例
	 * 
	 * @param dbpath
	 * @param db
	 * @param fileNumber
	 * @return
	 * @throws IOException
	 * @throws FileFormatException
	 */
	private LogEntity createLogEntity(String dbpath, LogIndex db, int fileNumber) throws IOException,
			FileFormatException {
		return new LogEntity(dbpath, db, fileNumber, this.fileLimitLength, fileRunner);
	}

	/**
	 * 一个文件的数据写入达到fileLimitLength的时候，滚动到下一个文件实例
	 * 
	 * @throws IOException
	 * @throws FileFormatException
	 */
	private void rotateNextLogWriter() throws IOException, FileFormatException {
		writerIndex = writerIndex + 1;
		writerHandle.putNextFile(writerIndex);
		if (readerHandle != writerHandle) {
			writerHandle.close();
		}
		db.putWriterIndex(writerIndex);
		writerHandle = createLogEntity(path + fileSeparator + filePrefix + "data_" + writerIndex + ".idb", db,
				writerIndex);
	}

	/**
	 * 向队列存储添加一个字符串
	 * 
	 * @param message
	 *            message
	 * @throws IOException
	 * @throws FileFormatException
	 */
	public void add(String message) throws IOException, FileFormatException {
		add(message.getBytes());
	}



	/**
	 * 向队列存储添加一个byte数组
	 * 
	 * @param message
	 * @throws IOException
	 * @throws FileFormatException
	 */

	public void add(byte[] message, int offset, int length) throws IOException, FileFormatException {
		short status = writerHandle.write(message, offset, length);
		if (status == LogEntity.WRITE_FULL) {
			rotateNextLogWriter();
			status = writerHandle.write(message, offset, length);
		}
		if (status == LogEntity.WRITE_SUCCESS) {
			db.incrementSize();
		}

	}
	public void add(byte[] message) throws IOException, FileFormatException {
		add(message, 0, message.length);
	}

	public byte[] peek() throws IOException, FileFormatException{
		byte[] b = null;
		try {
			b = readerHandle.peek();
		} catch (FileEOFException e) {
			int deleteNum = readerHandle.getCurrentFileNumber();
			int nextFile = readerHandle.getNextFile();
			readerHandle.close();
			fileRunner.deleteFile(path + fileSeparator + filePrefix + "data_" + deleteNum + ".idb");
			// 更新下一次读取的位置和索引
			db.putReaderPosition(LogEntity.messageStartPosition);
			db.putReaderIndex(nextFile);
			if (writerHandle.getCurrentFileNumber() == nextFile) {
				readerHandle = writerHandle;
			} else {
				readerHandle = createLogEntity(path + fileSeparator + filePrefix + "data_" + nextFile + ".idb", db,
						nextFile);
			}
			try {
				b = readerHandle.peek();
			} catch (FileEOFException e1) {
				log.error("read new log file FileEOFException error occurred", e1);
			}
		}
		return b;
	}
	public void remove() throws FileEOFException {
		if (readerHandle.remove()){
			db.decrementSize();
		}
	}
	/**
	 * 从队列存储中取出最先入队的数据，并移除它
	 * @return
	 * @throws IOException
	 * @throws FileFormatException
	 */
	public byte[] readNextAndRemove() throws IOException, FileFormatException {
		byte[] b = null;
		try {
			b = readerHandle.readNextAndRemove();
		} catch (FileEOFException e) {
			int deleteNum = readerHandle.getCurrentFileNumber();
			int nextFile = readerHandle.getNextFile();
			readerHandle.close();
			fileRunner.deleteFile(path + fileSeparator + filePrefix + "data_" + deleteNum + ".idb");
			// 更新下一次读取的位置和索引
			db.putReaderPosition(LogEntity.messageStartPosition);
			db.putReaderIndex(nextFile);
			if (writerHandle.getCurrentFileNumber() == nextFile) {
				readerHandle = writerHandle;
			} else {
				readerHandle = createLogEntity(path + fileSeparator + filePrefix + "data_" + nextFile + ".idb", db,
						nextFile);
			}
			try {
				b = readerHandle.readNextAndRemove();
			} catch (FileEOFException e1) {
				log.error("read new log file FileEOFException error occurred",e1);
			}
		}
		if (b != null) {
			db.decrementSize();
		}
		return b;
	}

	public void close() {
		log.debug("close fsQueue with path {}", path);
		db.close();
		readerHandle.close();
		writerHandle.close();
	}

	public int getQueueSize() {
		return db.getSize();
	}
}

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
package com.google.code.fqueue.log;

import com.google.code.fqueue.exception.FileEOFException;
import com.google.code.fqueue.exception.FileFormatException;
import com.google.code.fqueue.util.MappedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *@author sunli
 *@date 2011-5-18
 *@version $Id: LogEntity.java 2 2011-07-31 12:25:36Z sunli1223@gmail.com $
 */
public class LogEntity {
	private final Logger log = LoggerFactory.getLogger(LogEntity.class);
	public static final byte WRITE_SUCCESS = 1;
	public static final byte WRITE_FAILURE = 2;
	public static final byte WRITE_FULL = 3;
	public static final String MAGIC = "FQueuefs";
	public static int messageStartPosition = 20;
	private MappedFile mappedFile;
	private MappedFile.PageFlushService flushService;

	public MappedByteBuffer mappedByteBuffer;
	private int fileLimitLength = 1024 * 1024 * 40;

	private LogIndex db = null;
	/**
	 * 文件操作位置信息
	 */
	private String magicString = null;
	private int version = -1;
	private int readerPosition = -1;
	private AtomicInteger writerPosition = new AtomicInteger(-1);
	private int nextFile = -1;
	private int endPosition = -1;
	private int currentFileNumber = -1;

	public LogEntity(String path, LogIndex db, int fileNumber,
					 int fileLimitLength, FileRunner fileRunner) throws IOException, FileFormatException {
		this.currentFileNumber = fileNumber;
		this.fileLimitLength = fileLimitLength;
		this.db = db;
		mappedFile = new MappedFile(path, fileLimitLength);

		// 文件不存在，创建文件
		if (mappedFile.isNewCreated()) {
			createLogEntity();
			fileRunner.createFile(fileNumber + 1);
		} else {
			if (mappedFile.getRaFile().length() < LogEntity.messageStartPosition) {
				throw new FileFormatException("file format error");
			}
			mappedByteBuffer = mappedFile.getMappedByteBuffer();
			// magicString
			byte[] b = new byte[8];
			mappedByteBuffer.get(b);
			magicString = new String(b);
			if (magicString.equals(MAGIC) == false) {
				throw new FileFormatException("file format error");
			}
			// version
			version = mappedByteBuffer.getInt();
			// nextfile
			nextFile = mappedByteBuffer.getInt();
			endPosition = mappedByteBuffer.getInt();
			// 未写满
			if (endPosition == -1) {
				this.writerPosition.set(db.getWriterPosition()); ;
			} else if (endPosition == -2) {// 预分配的文件
				this.writerPosition.set(LogEntity.messageStartPosition);
				db.putWriterPosition(getWritePosition());
				mappedByteBuffer.position(16);
				mappedByteBuffer.putInt(-1);
				this.endPosition = -1;
			} else {
				this.writerPosition.set(endPosition);
			}
			if (db.getReaderIndex() == this.currentFileNumber) {
				this.readerPosition = db.getReaderPosition();
			} else {
				this.readerPosition = LogEntity.messageStartPosition;
			}
		}
		flushService = new MappedFile.PageFlushService(mappedFile, writerPosition);
		flushService.start();
	}

	public int getCurrentFileNumber() {
		return this.currentFileNumber;
	}

	public int getNextFile() {
		return this.nextFile;
	}

	private boolean createLogEntity() throws IOException {
		mappedByteBuffer = mappedFile.getMappedByteBuffer();
		mappedByteBuffer.put(MAGIC.getBytes());
		mappedByteBuffer.putInt(version);// 8 version
		mappedByteBuffer.putInt(nextFile);// 12next fileindex
		mappedByteBuffer.putInt(endPosition);// 16
		mappedByteBuffer.force();
		this.magicString = MAGIC;
		this.writerPosition.set(LogEntity.messageStartPosition);
		this.readerPosition = LogEntity.messageStartPosition;
		db.putWriterPosition(getWritePosition());
		return true;
	}

	/**
	 * 记录写位置
	 * 
	 * @param pos
	 */
	private void putWriterPosition(int pos) {
		db.putWriterPosition(pos);
	}

	private void putReaderPosition(int pos) {
		db.putReaderPosition(pos);
	}

	/**
	 * write next File number id.
	 * 
	 * @param number
	 */
	public void putNextFile(int number) {
		mappedByteBuffer.position(12);
		mappedByteBuffer.putInt(number);
		this.nextFile = number;
		flushService.wakeup();
	}

	public boolean isFull(int increment) {
		// confirm if the file is full
		if (this.fileLimitLength < getWritePosition() + increment) {
			return true;
		}
		return false;
	}

	private int getWritePosition() {
		return this.writerPosition.get();
	}

	public byte write(byte[] log, int offset, int length){
		int increment = length + 4;
		if (isFull(increment)) {
			mappedByteBuffer.position(16);
			mappedByteBuffer.putInt(getWritePosition());
			this.endPosition = this.getWritePosition();
			flushService.setFull(true);
			flushService.wakeup();
			return WRITE_FULL;
		}
		mappedByteBuffer.position(this.getWritePosition());
		mappedByteBuffer.putInt(length);
		mappedByteBuffer.put(log, offset, length);
		int pos = this.writerPosition.addAndGet(increment);
		putWriterPosition(pos);
		flushService.wakeup();
		return WRITE_SUCCESS;
	}

	public byte write(byte[] log) {
		return write(log, 0, log.length);
	}

	public byte[] peek() throws FileEOFException{
		if (isEOF()) {
			throw new FileEOFException("file eof");
		}
		// readerPosition must be less than writerPosition
		if (this.readerPosition >= this.getWritePosition()) {
			return null;
		}
		mappedByteBuffer.position(this.readerPosition);
		int length = mappedByteBuffer.getInt();
		byte[] b = new byte[length];
		mappedByteBuffer.get(b);
		return b;
	}

	/**
	 * remove the first element
	 * @throws FileEOFException
	 */
	public boolean remove() throws FileEOFException {
		if (isEOF()) {
			throw new FileEOFException("file eof");
		}
		// readerPosition must be less than writerPosition
		if (this.readerPosition >= this.getWritePosition()) {
			return false;
		}
		mappedByteBuffer.position(this.readerPosition);
		int length = mappedByteBuffer.getInt();
		this.readerPosition += length + 4;
		putReaderPosition(this.readerPosition);
		return true;
	}

	public byte[] readNextAndRemove() throws FileEOFException {
		if (isEOF()) {
			throw new FileEOFException("file eof");
		}
		// readerPosition must be less than writerPosition
		if (this.readerPosition >= this.getWritePosition()) {
			return null;
		}
		mappedByteBuffer.position(this.readerPosition);
		int length = mappedByteBuffer.getInt();
		byte[] b = new byte[length];
		this.readerPosition += length + 4;
		mappedByteBuffer.get(b);
		putReaderPosition(this.readerPosition);
		return b;
	}

	public boolean isEOF() {
		return this.endPosition != -1 && this.readerPosition >= this.endPosition;
	}

	public void close() {
		try {
			flushService.makeStop();
		    mappedFile.close();
		    flushService.shutdown();
		} catch (Exception e) {
			log.error("close logentity file error:", e);
		}
	}

	public String headerInfo() {
		StringBuilder sb = new StringBuilder();
		sb.append(" magicString:");
		sb.append(magicString);
		sb.append(" version:");
		sb.append(version);
		sb.append(" readerPosition:");
		sb.append(readerPosition);
		sb.append(" writerPosition:");
		sb.append(writerPosition);
		sb.append(" nextFile:");
		sb.append(nextFile);
		sb.append(" endPosition:");
		sb.append(endPosition);
		sb.append(" currentFileNumber:");
		sb.append(currentFileNumber);
		return sb.toString();
	}

}

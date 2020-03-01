package com.google.code.fqueue.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Author: shushenglin
 * Date:   2017/12/22 10:47
 */
public class MappedFile implements Closeable {
	private static final Logger logger = LoggerFactory.getLogger(MappedFile.class);

	public static final int DEFAULT_FILE_LIMIT = 1024 * 1024 * 100;
	private File file;
	private RandomAccessFile raFile;
	private FileChannel fc;
	private MappedByteBuffer mappedByteBuffer;
	private final int fileLimitLength;
	private boolean newCreated = false;
	private boolean writable = true;

	public MappedFile(File file, int fileLimit, boolean writable, boolean createIfNotExists) throws IOException{
		this.file = file;
		this.fileLimitLength = fileLimit;
		this.writable = writable;
		if (file.exists() == false){
			if (createIfNotExists == false) {
				throw new FileNotFoundException("file " + file.getAbsolutePath() + " not found");
			}
			createNewFile();
		}else{
			openFile();
		}
	}

	private void openFile() throws IOException{
		String mode = "rwd";
		FileChannel.MapMode mapMode = FileChannel.MapMode.READ_WRITE;
		if (!writable) {
			mode = "r";
			mapMode = FileChannel.MapMode.READ_ONLY;
		}
		raFile = new RandomAccessFile(file, mode);
		fc = raFile.getChannel();
		if (writable || this.fileLimitLength >= raFile.length()) {
			mappedByteBuffer = fc.map(mapMode, 0, this.fileLimitLength);
		}else{
			mappedByteBuffer = fc.map(mapMode, 0, raFile.length());
		}
	}

	public MappedFile(File file) throws IOException{
		this(file, DEFAULT_FILE_LIMIT, true, true);
	}

	public MappedFile(String path, int fileLimit) throws IOException{
		this(new File(path), fileLimit, true, true);
	}

	public File getFile() {
		return file;
	}

	public boolean isNewCreated() {
		return newCreated;
	}

	public int getFileLimitLength() {
		return fileLimitLength;
	}

	public MappedByteBuffer getMappedByteBuffer() {
		return mappedByteBuffer;
	}

	public RandomAccessFile getRaFile() {
		return raFile;
	}

	public FileChannel getFc() {
		return fc;
	}

	@Override
	public void close() {
		if (mappedByteBuffer == null) {
			return;
		}
		try {
			logger.debug("close file {}", file.getName());
			force();
			MappedByteBufferUtil.clean(mappedByteBuffer);
			mappedByteBuffer = null;
			fc.close();
			raFile.close();
		} catch (IOException ex) {
			logger.error("close mapped file {}", file, ex);
		}

	}

	public void force() {
		if (mappedByteBuffer != null) {
			mappedByteBuffer.force();
		}
	}

	private boolean createNewFile() throws IOException {
		if (file.createNewFile() == false) {
			return false;
		}
		openFile();
		newCreated = true;
		return true;
	}

	public static class FlushService extends ServiceThread {
		public FlushService(MappedFile file) {
			this.file = file;
		}

		private int flushInterval = 500;
		private final MappedFile file;

		public int getFlushInterval() {
			return flushInterval;
		}

		public void setFlushInterval(int flushInterval) {
			this.flushInterval = flushInterval;
		}

		@Override
		public String getServiceName() {
			return "RealTimeFlushService";
		}

		@Override
		public void run() {
			logger.info("flush service for mapped file {} start", file.getFile().getName());
			while (!this.isStopped()) {
				try {
					this.waitForRunning(flushInterval);
					long begin = System.currentTimeMillis();
					file.force();
					long past = System.currentTimeMillis() - begin;
					if (past > 500) {
						logger.info("Flush file {} data to disk costs {} ms",file.getFile().getName(), past);
					}
				} catch (Throwable t) {
					logger.error("service {} flush data with exception for file {}", getServiceName(), file.getFile().getName(), t);
				}
			}
			logger.info("flush service for mapped file {} end", file.getFile().getName());
		}
	}


	public static class SyncTask implements Runnable {

		private final MappedFile file;

		public SyncTask(MappedFile file) {
			this.file = file;
		}

		@Override
		public void run() {
//			logger.debug("run sync task for {}", file.getFile().getName());
			if (file != null) {
				try {
					file.force();
//					logger.debug("sync finish {}", file.getFile().getName());
				} catch (Exception e) {
					logger.error("force file {} error", file.getFile(), e);
				}
			}

		}
	}


	public class Sync implements Runnable {
		private final MappedFile file;

		public Sync(MappedFile file) {
			this.file = file;
		}
		@Override
		public void run() {
			while (true) {
				if (file != null) {
					try {
						file.force();
					} catch (Exception e) {
						break;
					}
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						break;
					}
				} else {
					break;
				}
			}

		}

	}
}

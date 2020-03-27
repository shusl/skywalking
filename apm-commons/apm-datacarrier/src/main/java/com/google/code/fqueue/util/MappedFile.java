package com.google.code.fqueue.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

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
	private volatile MappedByteBuffer mappedByteBuffer;
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

	public static class PageFlushService extends FlushService{
		public static final int OS_PAGE_SIZE = 1024 * 4;
		
		private final AtomicInteger writerPosition;
		private AtomicInteger flushedPosition = new AtomicInteger();
		private volatile boolean full = false;
		private int flushLeastPage = 1;

		public PageFlushService(MappedFile file, AtomicInteger writerPosition) {
			super(file);
			this.writerPosition = writerPosition;
			this.flushedPosition.set(writerPosition.get());
			String syncPagesStr = System.getProperty("FQueue.sync_pages", "4");
			flushLeastPage = Integer.parseInt(syncPagesStr);
		}

		@Override
		protected void doFlush() {
			if (isAbleToFlush(flushLeastPage)) {
				int writePos = writerPosition.get();
				logger.debug("flushing, flushed {} written {}", flushedPosition.get(), writePos);
				file.force();
				flushedPosition.set(writePos);
			}else{
				logger.debug("not able to flush, flushed {} written {}", flushedPosition.get(), writerPosition.get());
			}
		}

		public int getFlushLeastPage() {
			return flushLeastPage;
		}

		public void setFlushLeastPage(int flushLeastPage) {
			this.flushLeastPage = flushLeastPage;
		}

		public boolean isFull() {
			return full;
		}

		public void setFull(boolean full) {
			this.full = full;
		}

		private boolean isAbleToFlush(final int flushLeastPages) {
			int flush = this.flushedPosition.get();
			int write = writerPosition.get();

			if (this.isFull()) {
				return true;
			}

			if (flushLeastPages > 0) {
				return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
			}

			return write > flush;
		}

		@Override
		public String getServiceName() {
			return "PagedFlushService";
		}
	}

	public static class FlushService extends ServiceThread {
		private int flushInterval = 500;

		protected final MappedFile file;

		public FlushService(MappedFile file) {
			this.file = file;
		}

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
			String fileName = file.getFile().getName();
			logger.info("flush service for mapped file {} start", fileName);
			while (!this.isStopped()) {
				try {
					this.waitForRunning(flushInterval);
					logger.debug("try flush file {}", fileName);
					long begin = System.currentTimeMillis();
					doFlush();
					long past = System.currentTimeMillis() - begin;
					if (past > 500) {
						logger.info("Flush file {} data to disk costs {} ms", fileName, past);
					}
				} catch (Throwable t) {
					logger.error("service {} flush data with exception for file {}", getServiceName(), fileName, t);
				}
			}
			logger.info("flush service for mapped file {} end", fileName);
		}

		protected void doFlush() {
			file.force();
		}
	}


	public static class SyncTask implements Runnable {

		private final MappedFile file;

		public SyncTask(MappedFile file) {
			this.file = file;
		}

		@Override
		public void run() {
			logger.debug("run sync task for {}", file.getFile().getName());
			if (file != null) {
				try {
					file.force();
					logger.debug("sync finish {}", file.getFile().getName());
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

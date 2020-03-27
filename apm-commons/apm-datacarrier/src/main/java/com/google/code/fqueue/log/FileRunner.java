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

import com.google.code.fqueue.util.MappedByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author sunli
 * @date 2011-5-18
 * @version $Id: FileRunner.java 2 2011-07-31 12:25:36Z sunli1223@gmail.com $
 */
public class FileRunner {
    private final Logger log = LoggerFactory.getLogger(FileRunner.class);
    private String baseDir = null;
    private int fileLimitLength = 0;
    private ScheduledExecutorService scheduler;


	public void createFile(int fileNum) {
		this.scheduler.schedule(new CreateTask(fileNum), 0 , TimeUnit.MILLISECONDS);
	}

	public void deleteFile(String path) {
		this.scheduler.schedule(new DeleteTask(path), 0, TimeUnit.MILLISECONDS);
	}

    public FileRunner(String baseDir, int fileLimitLength, ScheduledExecutorService scheduledExecutorService) {
        this.baseDir = baseDir;
        this.fileLimitLength = fileLimitLength;
        this.scheduler = scheduledExecutorService;
    }


    class DeleteTask implements  Runnable{
    	private String filePath;

		public DeleteTask(String filePath) {
			this.filePath = filePath;
		}

		@Override
		public void run() {
			if (filePath != null) {
				File delFile = new File(filePath);
				delFile.delete();
			}
		}
	}

	class CreateTask implements Runnable {
    	private int fileNum;

		public CreateTask(int fileNum) {
			this.fileNum = fileNum;
		}

		@Override
		public void run() {
			String filePath = baseDir + fileNum + ".idb";
			try {
				create(filePath);
			} catch (IOException e) {
				log.error("预创建数据文件失败", e);
			}
		}
	}

	private boolean create(String path) throws IOException {

		File file = new File(path);
		if (file.exists() == false) {
			if (file.createNewFile() == false) {
				return false;
			}
			RandomAccessFile raFile = new RandomAccessFile(file, "rwd");
			FileChannel fc = raFile.getChannel();
			MappedByteBuffer mappedByteBuffer = fc.map(MapMode.READ_WRITE, 0, this.fileLimitLength);
			mappedByteBuffer.put(LogEntity.MAGIC.getBytes());
			mappedByteBuffer.putInt(1);// 8 version
			mappedByteBuffer.putInt(-1);// 12next fileindex
			mappedByteBuffer.putInt(-2);// 16
			mappedByteBuffer.force();
			MappedByteBufferUtil.clean(mappedByteBuffer);
			fc.close();
			raFile.close();
			return true;
		} else {
			return false;
		}
	}
}

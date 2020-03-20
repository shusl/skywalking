/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.apm.agent.core.remote;

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.DefaultImplementor;
import org.apache.skywalking.apm.agent.core.boot.ServiceManager;
import org.apache.skywalking.apm.agent.core.commands.CommandService;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.context.TracingContext;
import org.apache.skywalking.apm.agent.core.context.TracingContextListener;
import org.apache.skywalking.apm.agent.core.context.trace.TraceSegment;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.commons.datacarrier.DataCarrier;
import org.apache.skywalking.apm.commons.datacarrier.consumer.IConsumer;
import org.apache.skywalking.apm.network.common.Commands;
import org.apache.skywalking.apm.network.language.agent.UpstreamSegment;
import org.apache.skywalking.apm.network.language.agent.UpstreamSegmentList;
import org.apache.skywalking.apm.network.language.agent.v2.SegmentObject;
import org.apache.skywalking.apm.network.language.agent.v2.TraceSegmentReportServiceGrpc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.skywalking.apm.agent.core.conf.Config.Buffer.*;
import static org.apache.skywalking.apm.agent.core.remote.GRPCChannelStatus.CONNECTED;

@DefaultImplementor
public class UpstreamSegmentServiceClient implements BootService, IConsumer<UpstreamSegment>, TracingContextListener, GRPCChannelListener {
    private static final ILog logger = LogManager.getLogger(UpstreamSegmentServiceClient.class);

    private long lastLogTime;
    private long segmentUplinkedCounter;
    private long segmentAbandonedCounter;
    private DataCarrier<UpstreamSegment> carrier;
    private DataCarrier<UpstreamSegmentList> listCarrier;
    private DataCarrier<TraceSegment> segmentDataCarrier;
    private volatile TraceSegmentReportServiceGrpc.TraceSegmentReportServiceStub serviceStub;
    private volatile GRPCChannelStatus status = GRPCChannelStatus.DISCONNECT;
	private ScheduledExecutorService scheduler;
	private List<UpstreamSegment> upstreamSegments;

	@Override
    public void prepare() {
        ServiceManager.INSTANCE.findService(GRPCChannelManager.class).addChannelListener(this);
    }

    @Override
    public void boot() {
        lastLogTime = System.currentTimeMillis();
        segmentUplinkedCounter = 0;
        segmentAbandonedCounter = 0;
		if (Config.FQueue.DISABLED) {
			initWithoutFQueue();
		}else{
			initWithFQueue();
		}
	}

	private void initWithoutFQueue() {
		logger.info("init data carrier without FQueue with buffer size {}", BUFFER_SIZE);
		segmentDataCarrier = new DataCarrier<TraceSegment>(CHANNEL_SIZE, BUFFER_SIZE, BLOCK_STRATEGY);
		segmentDataCarrier.consume(new AbstractConsumer<TraceSegment>() {
			@Override
			public void consume(List<TraceSegment> data) {
				consumeTraceSegment(data);
			}
		}, 1, CONSUMER_CYCLE);
	}

	private void initWithFQueue() {
		scheduler = new ScheduledThreadPoolExecutor(Config.FQueue.THREADS);
		if (Config.FQueue.PACK_SEGMENTS) {
			logger.info("init fQueue carrier with pack segments");
			listCarrier = new DataCarrier<UpstreamSegmentList>("FQueue", new UpstreamSegmentsCodec(), scheduler,
					Config.FQueue.DB_PATH, Config.FQueue.PACKED_BATCH_SIZE, Config.FQueue.LOG_SIZE);
			listCarrier.consume(new AbstractConsumer<UpstreamSegmentList>() {
				@Override
				public void consume(List<UpstreamSegmentList> data) {
					consumeUpstreamSegments(data);
				}
			}, 1, Config.FQueue.CONSUMER_CYCLE);
		}else {
			carrier = new DataCarrier<UpstreamSegment>("FQueue", new UpstreamSegmentCodec(), scheduler,
					Config.FQueue.DB_PATH, Config.FQueue.BATCH_SIZE, Config.FQueue.LOG_SIZE);
			carrier.consume(this, 1, Config.FQueue.CONSUMER_CYCLE);
		}
		logger.info("init data carrier with buffer size {}", BUFFER_SIZE);
		segmentDataCarrier = new DataCarrier<TraceSegment>(CHANNEL_SIZE, BUFFER_SIZE, BLOCK_STRATEGY);
		segmentDataCarrier.consume(new AbstractConsumer<TraceSegment>() {
			@Override
			public void consume(List<TraceSegment> data) {
				try {
					if (Config.FQueue.PACK_SEGMENTS) {
						UpstreamSegmentList.Builder builder = UpstreamSegmentList.newBuilder();
						for (TraceSegment segment : data) {
							UpstreamSegment upstreamSegment = segment.transform();
							builder.addSegments(upstreamSegment);
						}
						listCarrier.produce(builder.build());
					}else{
						for (TraceSegment segment : data) {
							UpstreamSegment upstreamSegment = segment.transform();
							carrier.produce(upstreamSegment);
						}
					}
				} catch (Throwable t) {
					logger.error(t, "Transform and send segment to fQueue fail.");
				}
			}
		}, 1, CONSUMER_CYCLE) ;
	}

	@Override
    public void onComplete() {
        TracingContext.ListenerManager.add(this);
    }

    @Override
    public void shutdown() {
		logger.info("shutdown service client");
        TracingContext.ListenerManager.remove(this);
		segmentDataCarrier.shutdownConsumers();
		if (carrier != null){
			carrier.shutdownConsumers();
		}
		if (listCarrier != null) {
			listCarrier.shutdownConsumers();
		}
		if (scheduler != null) {
			scheduler.shutdown();
		}
	}

    @Override
    public void init() {

    }

    @Override
    public void consume(List<UpstreamSegment> data) {
		consumeUpstreamSegment(data);
	}

	private void consumeUpstreamSegments(List<UpstreamSegmentList> data) {
		if (upstreamSegments == null){
			int totalSize = 0;
			for (UpstreamSegmentList list : data) {
				totalSize += list.getSegmentsCount();
			}
			upstreamSegments = new ArrayList<UpstreamSegment>(totalSize);
		}
		for (UpstreamSegmentList segmentList : data) {
			upstreamSegments.addAll(segmentList.getSegmentsList());
		}
		try{
			consumeUpstreamSegment(upstreamSegments);
		}finally {
			upstreamSegments.clear();
		}
	}

	private void consumeTraceSegment(List<TraceSegment> data){
		if (CONNECTED.equals(status)) {
			List<UpstreamSegment> dataUpstream = new ArrayList<UpstreamSegment>(data.size());
			for (TraceSegment segment : data) {
				dataUpstream.add(segment.transform());
			}
			consumeUpstreamSegment(dataUpstream);
		} else {
			segmentAbandonedCounter += data.size();
		}
		printUplinkStatus();
	}

	private void consumeUpstreamSegment(List<UpstreamSegment> data) {
		if (CONNECTED.equals(status)) {
			final GRPCStreamServiceStatus status = new GRPCStreamServiceStatus(false);
			StreamObserver<UpstreamSegment> upstreamSegmentStreamObserver = serviceStub.withDeadlineAfter(
				Config.Collector.GRPC_UPSTREAM_TIMEOUT, TimeUnit.SECONDS
			).collect(new StreamObserver<Commands>() {
				@Override
				public void onNext(Commands commands) {
					ServiceManager.INSTANCE.findService(CommandService.class)
										   .receiveCommand(commands);
				}

				@Override
				public void onError(
					Throwable throwable) {
					status.finished();
					if (logger.isErrorEnable()) {
						logger.error(
							throwable,
							"Send UpstreamSegment to collector fail with a grpc internal exception."
						);
					}
					ServiceManager.INSTANCE
						.findService(GRPCChannelManager.class)
						.reportError(throwable);
				}

				@Override
				public void onCompleted() {
					status.finished();
				}
			});

			try {
				for (UpstreamSegment segment : data) {
					upstreamSegmentStreamObserver.onNext(segment);
				}
			} catch (Throwable t) {
				logger.error(t, "Transform and send UpstreamSegment to collector fail.");
			}

			upstreamSegmentStreamObserver.onCompleted();

			status.wait4Finish();
			segmentUplinkedCounter += data.size();
		} else {
			segmentAbandonedCounter += data.size();
		}

		printUplinkStatus();
	}

	private void printUplinkStatus() {
        long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis - lastLogTime > 30 * 1000) {
            lastLogTime = currentTimeMillis;
            if (segmentUplinkedCounter > 0) {
                logger.debug("{} trace segments have been sent to collector.", segmentUplinkedCounter);
                segmentUplinkedCounter = 0;
            }
            if (segmentAbandonedCounter > 0) {
                logger.debug(
                    "{} trace segments have been abandoned, cause by no available channel.", segmentAbandonedCounter);
                segmentAbandonedCounter = 0;
            }
        }
    }

    @Override
    public void onError(List<UpstreamSegment> data, Throwable t) {
        logger.error(t, "Try to send {} trace segments to collector, with unexpected exception.", data.size());
    }

    @Override
    public void onExit() {

    }

    abstract class AbstractConsumer<T> implements IConsumer<T>{
		@Override
		public void init() {
		}

		@Override
		public void onError(List<T> data, Throwable t) {
			logger.error(t, "Try to send {} trace segments to collector, with unexpected exception.", data.size());
		}

		@Override
		public void onExit() {

		}
	}

    @Override
    public void afterFinished(TraceSegment traceSegment) {
        if (traceSegment.isIgnore()) {
            return;
        }
        if (!segmentDataCarrier.produce(traceSegment)) {
            if (logger.isDebugEnable()) {
                logger.debug("One trace segment has been abandoned, cause by buffer is full.");
            }
        }
    }

    @Override
    public void statusChanged(GRPCChannelStatus status) {
        if (CONNECTED.equals(status)) {
            Channel channel = ServiceManager.INSTANCE.findService(GRPCChannelManager.class).getChannel();
            serviceStub = TraceSegmentReportServiceGrpc.newStub(channel);
        }
        this.status = status;
    }
}

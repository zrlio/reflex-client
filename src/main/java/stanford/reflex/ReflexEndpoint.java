/*
 * ReflexClient: An NIO-based Reflex client library
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package stanford.reflex;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;

import stanford.reflex.ReflexChannel.MessageType;

public class ReflexEndpoint extends ReflexChannel {
	private static final Logger LOG = ReflexUtils.getLogger();
	
	private ReflexClientGroup group;
	private ConcurrentHashMap<Long, ReflexFuture> pendingRPCs;
	private ArrayBlockingQueue<ByteBuffer> bufferQueue;	
	private ByteBuffer responseBuffer;
	private ReflexHeader header;
	private AtomicLong sequencer;
	private SocketChannel channel;
	private ReentrantLock readLock;
	private ReentrantLock writeLock;

	public ReflexEndpoint(ReflexClientGroup group, SocketChannel channel) throws Exception {
		super(group.getBlockSize());
		this.group = group;
		this.channel = channel;
		this.pendingRPCs = new ConcurrentHashMap<Long, ReflexFuture>();
		this.readLock = new ReentrantLock();
		this.writeLock = new ReentrantLock();
		this.bufferQueue = new ArrayBlockingQueue<ByteBuffer>(group.getQueueDepth()+1);
		for (int i = 0; i < group.getQueueDepth()+1; i++){
			ByteBuffer buffer = ByteBuffer.allocate(ReflexChannel.HEADERSIZE);
			buffer.order(ByteOrder.LITTLE_ENDIAN);
			bufferQueue.put(buffer);
		}	
		this.responseBuffer = bufferQueue.poll();
		this.header = new ReflexHeader();
		this.sequencer = new AtomicLong(1);
	}

	public void connect(InetSocketAddress address) throws IOException {
		LOG.info("connection to " + address.toString());
		this.channel.connect(address);
		this.channel.socket().setTcpNoDelay(group.isNodelay());
		this.channel.configureBlocking(false);		
	}

	public ReflexFuture issueRequest(MessageType type, long lba, int count, ByteBuffer buffer) throws IOException {
		ByteBuffer requestBuffer = getBuffer();
		long ticket = sequencer.getAndIncrement();
		makeRequest(type, ticket, lba, count, requestBuffer);
		ReflexFuture future = new ReflexFuture(this, ticket, buffer);
		pendingRPCs.put(ticket, future);
		while(!tryTransmitting(requestBuffer)){
		}
		putBuffer(requestBuffer);
		return future;
	}
	
	public void close() throws IOException{
		this.channel.close();
	}

	public String address() throws IOException {
		return channel.getRemoteAddress().toString();
	}

	public ReflexClientGroup getGroup() {
		return group;
	}

	void pollResponse(AtomicBoolean done) throws IOException {
		boolean locked = readLock.tryLock();
		if (locked) {
			if (!done.get()){
				fetchHeader(channel, responseBuffer, header);
				ReflexFuture future = pendingRPCs.remove(header.getTicket());
				fetchBuffer(channel, header, future.getBuffer());
				future.signal();
			} 
			readLock.unlock();
		} 
	}

	private boolean tryTransmitting(ByteBuffer buffer) throws IOException{
		boolean locked = writeLock.tryLock();
		if (locked) {
			transmitMessage(channel, buffer);
			writeLock.unlock();
			return true;
		} 		
		return false;
	}
	
	private ByteBuffer getBuffer(){
		ByteBuffer buffer = bufferQueue.poll();
		while(buffer == null){
			buffer = bufferQueue.poll();
		}
		return buffer;
	}
	
	private void putBuffer(ByteBuffer buffer){
		bufferQueue.add(buffer);
	}
}

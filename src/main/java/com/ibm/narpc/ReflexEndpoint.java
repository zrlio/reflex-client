/*
 * NaRPC: An NIO-based RPC library
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

package com.ibm.narpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.ibm.narpc.ReflexChannel.MessageType;

public class ReflexEndpoint extends ReflexChannel {
	private ReflexClientGroup group;
	private ConcurrentHashMap<Long, ReflexFuture> pendingRPCs;
	private ArrayBlockingQueue<ByteBuffer> bufferQueue;	
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
		this.bufferQueue = new ArrayBlockingQueue<ByteBuffer>(group.getQueueDepth());
		for (int i = 0; i < group.getQueueDepth(); i++){
			ByteBuffer buffer = ByteBuffer.allocate(ReflexChannel.HEADERSIZE);
			bufferQueue.put(buffer);
		}	
		this.sequencer = new AtomicLong(1);
	}

	public ReflexFuture issueRequest(MessageType type, long lba, int count, ByteBuffer responseBuffer) throws IOException {
		ByteBuffer requestBuffer = getBuffer();
		long ticket = sequencer.getAndIncrement();
		makeRequest(type, ticket, lba, count, requestBuffer);
		ReflexFuture future = new ReflexFuture(this, responseBuffer, ticket);
		pendingRPCs.put(ticket, future);
		while(!tryTransmitting(requestBuffer)){
		}
		putBuffer(requestBuffer);
		return future;
	}
	
	public void pollResponse(ByteBuffer responseBuffer, AtomicBoolean done) throws IOException {
		boolean locked = readLock.tryLock();
		if (locked) {
			if (!done.get()){
				long ticket = fetchBuffer(channel, responseBuffer);
				ReflexFuture future = pendingRPCs.remove(ticket);
				future.signal();
			} 
			readLock.unlock();
		} 
	}

	public void connect(InetSocketAddress address) throws IOException {
		this.channel.connect(address);
		this.channel.socket().setTcpNoDelay(group.isNodelay());
		this.channel.configureBlocking(false);		
	}	
	
	public void close() throws IOException{
		this.channel.close();
	}
	
	public String address() throws IOException {
		return channel.getRemoteAddress().toString();
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

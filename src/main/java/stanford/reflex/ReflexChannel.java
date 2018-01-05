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
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import org.slf4j.Logger;

//typedef struct __attribute__ ((__packed__)) {
//	  uint16_t magic;
//	  uint16_t opcode;
//	  void *req_handle;
//	  unsigned long lba;
//	  unsigned int lba_count;
//	} binary_header_blk_t;

public abstract class ReflexChannel {
	private static final Logger LOG = ReflexUtils.getLogger();
	static final int HEADERSIZE = Short.BYTES + Short.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES;
	enum MessageType { GET, PUT }
	
	private int blockSize;
	
	public ReflexChannel(int blockSize){
		this.blockSize = blockSize;
	}
	
	public void makeRequest(MessageType type, long ticket, long lba, int count, ByteBuffer buffer) throws IOException {
		buffer.clear();
		buffer.putShort((short) HEADERSIZE);
		buffer.putShort((short) type.ordinal());
		buffer.putLong(ticket);
		buffer.putLong(lba);
		buffer.putInt(count);
		buffer.clear().limit(HEADERSIZE);
	}	
	
	public long fetchBuffer(SocketChannel channel, ByteBuffer buffer) throws IOException{
		buffer.clear().limit(HEADERSIZE);
		while (buffer.hasRemaining()) {
			if (channel.read(buffer) < 0){
				return -1;
			}
		}
		buffer.flip();
		
		short magic = buffer.getShort();
		short type = buffer.getShort();
		long ticket = buffer.getLong();
		long lba = buffer.getLong();
		int count = buffer.getInt();
		
		buffer.clear().limit(count*blockSize);
		while (buffer.hasRemaining()) {
			if (channel.read(buffer) < 0) {
				throw new IOException("error when reading header from socket");
			}
			
		}
		buffer.flip();
//		LOG.info("fetching message with ticket " + ticket + ", threadid " + Thread.currentThread().getName());
		return ticket;
	}
	
	public void transmitMessage(SocketChannel channel, ByteBuffer buffer) throws IOException {
//		LOG.info("transmitting message with magic2 " + buffer.getShort(0) + ", type " + buffer.getShort(2) + ", ticket " + buffer.getLong(4) + ", threadid " + Thread.currentThread().getName());
		while(buffer.hasRemaining()){
			channel.write(buffer);
		}		
	}
	
}

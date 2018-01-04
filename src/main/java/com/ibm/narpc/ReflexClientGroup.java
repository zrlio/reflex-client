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

import java.nio.channels.SocketChannel;

public class ReflexClientGroup<R extends ReflexMessage, T extends ReflexMessage> extends ReflexGroup {
	
	public ReflexClientGroup() {
		super();
	}	
	
	public ReflexClientGroup(int queueDepth, int messageSize, boolean nodelay) {
		super(queueDepth, messageSize, nodelay);
	}

	public ReflexEndpoint<R,T> createEndpoint() throws Exception{
		return new ReflexEndpoint<R,T>(this, SocketChannel.open());
	}
}

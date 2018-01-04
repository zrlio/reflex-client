package com.ibm.narpc;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.ibm.narpc.ReflexChannel.MessageType;
import com.ibm.narpc.ReflexFuture;
import com.ibm.narpc.ReflexClientGroup;
import com.ibm.narpc.ReflexEndpoint;

public class SimpleReflexClient implements Runnable {
	private int id;
	private ReflexEndpoint endpoint;
	private int queueDepth;
	private int batchCount;
	private int loopCount;
	
	public SimpleReflexClient(int id, ReflexEndpoint endpoint, int queueDepth, int batchCount, int loopCount){
		this.id = id;
		this.endpoint = endpoint;
		this.queueDepth = queueDepth;
		this.batchCount = batchCount;
		this.loopCount = loopCount;
	}

	public void run() {
		try {
			System.out.println("SimpleRPCClient, queueDepth " + queueDepth + ", batchCount " + batchCount + ", loopCount " + loopCount);
			ArrayList<ReflexFuture> futureList = new ArrayList<ReflexFuture>(batchCount);
			for(int i = 0; i < loopCount; i++){
				futureList.clear();
				for (int j = 0; j < batchCount; j++){
					ReflexFuture future = endpoint.issueRequest(MessageType.GET, 0, 1, ByteBuffer.allocate(512*1024));
					futureList.add(j, future);
				}
				for (ReflexFuture future: futureList){
					future.get();
//					System.out.println("id " + id + " got response, value " + future.get().getResult() + ", ticket " + future.getTicket());
				}
			}
		} catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception{
		int queueDepth = ReflexClientGroup.DEFAULT_QUEUE_DEPTH;
		int loop = queueDepth;
		int batchCount = queueDepth;
		int threadCount = 1;
		
		if (args != null) {
			Option queueOption = Option.builder("q").desc("queue length").hasArg().build();
			Option loopOption = Option.builder("k").desc("loop").hasArg().build();
			Option threadOption = Option.builder("n").desc("number of threads").hasArg().build();
			Option batchOption = Option.builder("b").desc("batch of RPCs").hasArg().build();
			Options options = new Options();
			options.addOption(queueOption);
			options.addOption(loopOption);
			options.addOption(threadOption);
			options.addOption(batchOption);
			CommandLineParser parser = new DefaultParser();

			try {
				CommandLine line = parser.parse(options, Arrays.copyOfRange(args, 0, args.length));
				if (line.hasOption(queueOption.getOpt())) {
					queueDepth = Integer.parseInt(line.getOptionValue(queueOption.getOpt()));
				}
				if (line.hasOption(loopOption.getOpt())) {
					loop = Integer.parseInt(line.getOptionValue(loopOption.getOpt()));
				}	
				if (line.hasOption(threadOption.getOpt())) {
					threadCount = Integer.parseInt(line.getOptionValue(threadOption.getOpt()));
				}	
				if (line.hasOption(batchOption.getOpt())) {
					batchCount = Integer.parseInt(line.getOptionValue(batchOption.getOpt()));
				}					
			} catch (ParseException e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("TCP RPC", options);
				System.exit(-1);
			}
		}	
		
		ReflexClientGroup clientGroup = new ReflexClientGroup(queueDepth, ReflexClientGroup.DEFAULT_MESSAGE_SIZE, true);
		ReflexEndpoint endpoint = clientGroup.createEndpoint();
		InetSocketAddress address = new InetSocketAddress("localhost", 1234);
		endpoint.connect(address);	
		Thread[] threads = new Thread[threadCount];
		for (int i = 0; i < threadCount; i++){
			SimpleReflexClient client = new SimpleReflexClient(i, endpoint, queueDepth, batchCount, loop);
			threads[i] = new Thread(client);
			threads[i].start();
		}
		for (int i = 0; i < threadCount; i++){
			threads[i].join();
		}
	}
}

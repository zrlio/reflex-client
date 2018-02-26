package stanford.reflex;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;

import com.ibm.reflex.client.ReflexClientGroup;
import com.ibm.reflex.client.ReflexEndpoint;
import com.ibm.reflex.client.ReflexFuture;
import com.ibm.reflex.client.ReflexUtils;

public class SimpleReflexWriter implements Runnable {
	private static final Logger LOG = ReflexUtils.getLogger();
	
	private int id;
	private ReflexEndpoint endpoint;
	private int queueDepth;
	private int batchCount;
	private int loopCount;
	private int size;
	private int count;
	private ArrayBlockingQueue<ByteBuffer> bufferQueue;	
	
	public SimpleReflexWriter(int id, ReflexEndpoint endpoint, int queueDepth, int batchCount, int loopCount, int size) throws InterruptedException{
		this.id = id;
		this.endpoint = endpoint;
		this.queueDepth = queueDepth;
		this.batchCount = batchCount;
		this.loopCount = loopCount;
		this.size = size;
		this.count = size/endpoint.getGroup().getBlockSize();
		this.bufferQueue = new ArrayBlockingQueue<ByteBuffer>(batchCount);
		for (int i = 0; i < batchCount; i++){
			ByteBuffer buffer = ByteBuffer.allocate(size);
			buffer.order(ByteOrder.LITTLE_ENDIAN);
			bufferQueue.put(buffer);
		}		
	}

	public void run() {
		try {
			LOG.info("SimpleReflexWriter 1.4, queueDepth " + queueDepth + ", batchCount " + batchCount + ", loopCount " + loopCount + ", size " + size + ", count " + count);
			ArrayBlockingQueue<ReflexFuture> futureList = new ArrayBlockingQueue<ReflexFuture>(batchCount);
			long start = System.currentTimeMillis();
			double ops = 0.0;
			int lba = 0;
			for (int i = 0; i < batchCount; i++){
				ByteBuffer payload = bufferQueue.take();
				payload.clear();
				ReflexFuture future = endpoint.put(lba, payload);
				futureList.add(future);
				ops += 1.0;
				lba += count;
			}
			for(int i = 0; i < loopCount - batchCount; i++){
				ReflexFuture future = futureList.poll();
				future.get();
				ByteBuffer payload = future.getBuffer();
				payload.clear();
				future = endpoint.put(lba, payload);
				futureList.add(future);
				ops += 1.0;
				lba += count;
			}			
			while(!futureList.isEmpty()){
				ReflexFuture future = futureList.poll();
				if (!future.isDone()){
					futureList.add(future);
				}
			}			
			long end = System.currentTimeMillis();
			double executionTime = ((double) (end - start)) / 1000.0;
			double sumbytes = ops*size;
			double throughput = 0.0;
			double latency = 0.0;
			double sumbits = sumbytes * 8.0;
			if (executionTime > 0) {
				throughput = sumbits / executionTime / 1000.0 / 1000.0;
				latency = 1000000.0 * executionTime / ops;
			}
			LOG.info("execution time " + executionTime);
			LOG.info("ops " + ops);
			LOG.info("sumbytes " + sumbytes);
			LOG.info("throughput " + throughput);
			LOG.info("latency " + latency);			
		} catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception{
		int queueDepth = ReflexClientGroup.DEFAULT_QUEUE_DEPTH;
		int loop = queueDepth;
		int batchCount = queueDepth;
		int threadCount = 1;
		String ipAddress = "localhost";
		int port = 1234;
		int size = ReflexClientGroup.DEFAULT_BLOCK_SIZE;
		boolean noDelay = false;
		
		if (args != null) {
			Option queueOption = Option.builder("q").desc("queue length").hasArg().build();
			Option loopOption = Option.builder("k").desc("loop").hasArg().build();
			Option batchOption = Option.builder("b").desc("batch of RPCs").hasArg().build();
			Option addressOption = Option.builder("a").desc("address of reflex server").hasArg().build();
			Option portOption = Option.builder("p").desc("port of reflex server").hasArg().build();
			Option sizeOption = Option.builder("s").desc("size").hasArg().build();
			Option noDelayOption = Option.builder("d").desc("nodelay").hasArg().build();
			Options options = new Options();
			options.addOption(queueOption);
			options.addOption(loopOption);
			options.addOption(batchOption);
			options.addOption(addressOption);
			options.addOption(portOption);
			options.addOption(sizeOption);
			options.addOption(noDelayOption);
			CommandLineParser parser = new DefaultParser();

			try {
				CommandLine line = parser.parse(options, Arrays.copyOfRange(args, 0, args.length));
				if (line.hasOption(queueOption.getOpt())) {
					queueDepth = Integer.parseInt(line.getOptionValue(queueOption.getOpt()));
				}
				if (line.hasOption(loopOption.getOpt())) {
					loop = Integer.parseInt(line.getOptionValue(loopOption.getOpt()));
				}	
				if (line.hasOption(batchOption.getOpt())) {
					batchCount = Integer.parseInt(line.getOptionValue(batchOption.getOpt()));
				}	
				
				if (line.hasOption(addressOption.getOpt())) {
					ipAddress = line.getOptionValue(addressOption.getOpt());
				}
				if (line.hasOption(portOption.getOpt())) {
					port = Integer.parseInt(line.getOptionValue(portOption.getOpt()));
				}		
				if (line.hasOption(sizeOption.getOpt())) {
					size = Integer.parseInt(line.getOptionValue(sizeOption.getOpt()));
				}		
				if (line.hasOption(noDelayOption.getOpt())) {
					noDelay = Boolean.parseBoolean(line.getOptionValue(noDelayOption.getOpt()));
				}				
			} catch (ParseException e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("TCP RPC", options);
				System.exit(-1);
			}
		}	
		
		ReflexClientGroup clientGroup = new ReflexClientGroup(queueDepth, ReflexClientGroup.DEFAULT_BLOCK_SIZE, noDelay);
		ReflexEndpoint endpoint = clientGroup.createEndpoint();
		InetSocketAddress address = new InetSocketAddress(ipAddress, port);
		endpoint.connect(address);	
		SimpleReflexWriter client = new SimpleReflexWriter(0, endpoint, queueDepth, batchCount, loop, size);
		client.run();
	}
}

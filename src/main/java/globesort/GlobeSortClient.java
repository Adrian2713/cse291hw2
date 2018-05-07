package globesort;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.*;
import net.sourceforge.argparse4j.inf.*;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.lang.RuntimeException;
import java.lang.Exception;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Date;

public class GlobeSortClient {

    private final ManagedChannel serverChannel;
    private final GlobeSortGrpc.GlobeSortBlockingStub serverStub;

	private static int MAX_MESSAGE_SIZE = 100 * 1024 * 1024;

    private String serverStr;

    public GlobeSortClient(String ip, int port) {
        this.serverChannel = ManagedChannelBuilder.forAddress(ip, port)
				.maxInboundMessageSize(MAX_MESSAGE_SIZE)
                .usePlaintext(true).build();
        this.serverStub = GlobeSortGrpc.newBlockingStub(serverChannel);

        this.serverStr = ip + ":" + port;
    }

    public void run(Integer[] values, Integer numValues) throws Exception {
    		Date startLatencyDate = new Date();
    		long startLatency = startLatencyDate.getTime();
    		
    		
        System.out.println("Pinging " + serverStr + "...");
        serverStub.ping(Empty.newBuilder().build());
        System.out.println("Ping successful.");

        //latency
        Date endLatencyTimeDate = new Date();
        long latency = endLatencyTimeDate.getTime() - startLatency;
        System.out.print("latency is : ");
        System.out.println(latency);
        
        
        System.out.println("Requesting server to sort array");
        Date startAppThroughputDate = new Date();
        long startAppThroughputTime = startAppThroughputDate.getTime();
        IntArray request = IntArray.newBuilder().addAllValues(Arrays.asList(values)).build();
        IntArray response = serverStub.sortIntegers(request);
        
        
        Date endAppThroughputDate = new Date();
        Integer appThroughputTime = (int) (endAppThroughputDate.getTime() - startAppThroughputTime);
        Integer appThroughput = numValues / appThroughputTime * 1000;
        Integer[] responseNo = response.getValuesList().toArray(new Integer[response.getValuesList().size()]);
        
        //period
        System.out.print("duration of sorting = ");
        Integer period = responseNo[responseNo.length - 1];
        System.out.println(period);
        
        //app throughput
        
        System.out.print("app throughput is : ");
        System.out.println(appThroughput);
        
        //network throughput
        Integer netThroughput = numValues / ((appThroughputTime - period) / 2) * 1000;
        System.out.print("network throughput is : ");
        System.out.println(netThroughput);
        
        
        System.out.println("Sorted array");
    }

    public void shutdown() throws InterruptedException {
        serverChannel.shutdown().awaitTermination(2, TimeUnit.SECONDS);
    }

    private static Integer[] genValues(int numValues) {
        ArrayList<Integer> vals = new ArrayList<Integer>();
        Random randGen = new Random();
        for(int i : randGen.ints(numValues).toArray()){
            vals.add(i);
        }
        return vals.toArray(new Integer[vals.size()]);
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("GlobeSortClient").build()
                .description("GlobeSort client");
        parser.addArgument("server_ip").type(String.class)
                .help("Server IP address");
        parser.addArgument("server_port").type(Integer.class)
                .help("Server port");
        parser.addArgument("num_values").type(Integer.class)
                .help("Number of values to sort");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace cmd_args = parseArgs(args);
        if (cmd_args == null) {
            throw new RuntimeException("Argument parsing failed");
        }

        Integer numValues= cmd_args.getInt("num_values");
        Integer[] values = genValues(numValues);
        

        GlobeSortClient client = new GlobeSortClient(cmd_args.getString("server_ip"), cmd_args.getInt("server_port"));
        try {
            client.run(values, numValues);
        } finally {
            client.shutdown();
        }
    }
    
}

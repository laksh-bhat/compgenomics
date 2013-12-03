package project.cs439.topology;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import org.apache.thrift7.TException;

import java.io.IOException;


/**
 * User: lbhat@damsl
 * Date: 11/18/13
 * Time: 9:16 PM
 */
public class DrpcQueryRunner {
    public static void main (final String[] args) throws IOException, TException, DRPCExecutionException {
        if (args.length < 1){
            System.err.println("Where are the arguments?");
            return ;
	}

        long duration = 0;
        final DRPCClient drpcClient1 = new DRPCClient("qp-hd7", 3772, 90000000);
//        final DRPCClient drpcClient2 = new DRPCClient("qp-hd6", 3772, 90000000);
        long startTime = System.currentTimeMillis();
        String result = runQuery(args[0], drpcClient1);
/*
        new Thread("secondDrpc"){
            @Override
            public void run () {
                try {
                    String result2 = runQuery(args[0], drpcClient2);
                    System.out.println(result2);
                } catch ( TException e ) {
                    e.printStackTrace(); 
                } catch ( DRPCExecutionException e ) {
                    e.printStackTrace();
                }
            }
        }.run();
*/


        long endTime = System.currentTimeMillis();
        duration += endTime - startTime;
        System.out.println(result);
        drpcClient1.close();
//        drpcClient2.close();
    }

    private static String runQuery (final String topologyAndDrpcServiceName, final DRPCClient client) throws TException, DRPCExecutionException {/*Query Arguments in order -- marketsegment, orderdate, shipdate*/
        return client.execute(topologyAndDrpcServiceName, "1080548553,19950315,19950315");
    }
}


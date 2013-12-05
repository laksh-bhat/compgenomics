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
        if (args.length < 1) {
            System.err.println("Where are the arguments?");
            return;
        }

        final DRPCClient client = new DRPCClient("qp-hd7", 3772, 10000 /*timeout*/);
        runQuery(args[0], client);
        client.close();
    }

    private static String runQuery (final String topologyAndDrpcServiceName, final DRPCClient client) throws
    TException,
    DRPCExecutionException
    {
        return client.execute(topologyAndDrpcServiceName, "Idiot! You don't care what the arguments are :)");
    }
}


package asgmt5;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * User: lbhat@damsl
 * Date: 11/17/13
 * Time: 12:58 AM
 */
public class DeBruijnGraphDrawer {

    public static void main (String[] args) {
        //String[] reads = {"ABCDEFGC", "EFGCDHIJ", "CDEFGCDH"};
        String[] reads = {"ABFABGABABEABG"};
        int k = 3;
        Map<String, Integer> deBruijn = new HashMap<String, Integer>();
        for (String read : reads) {
            for (int i = 0; i + k - 1 < read.length(); i++) {
                String src = read.substring(i, i + k - 1);
                String dest = read.substring(i + 1, i + k);
                String key = src + "," + dest;
                if (deBruijn.containsKey(key))
                    deBruijn.put(key, deBruijn.get(key) + 1);
                else
                    deBruijn.put(key, 1);
            }
        }
        System.out.println("digraph G {");
        System.out.println("  size =\"4,4\";");
        System.out.println("  edge [color=red];");

        for (String key : deBruijn.keySet()) {
            String[] keys = key.split(",");
            System.out.println(MessageFormat.format("  {0}->{1}[label=\"{2}\"];", keys[0], keys[1], deBruijn.get(key)));
        }
        System.out.println("}");

    }

}

package project.cs439.test;

import project.cs439.function.CorrectionFunction;
import project.cs439.state.StatisticsState;

import java.util.HashMap;
import java.util.Map;

/**
 * User: lbhat@damsl
 * Date: 11/27/13
 * Time: 2:48 AM
 */
public class TestCorrectors {
    public static void main (String[] args) {
        StatisticsState.k = 3;
        Map<String, Double> trusted = new HashMap<String, Double>();
        trusted.put("ACA", 4D);
        trusted.put("CAA", 2D);
        trusted.put("AAC", 5D);
        double[][][] conditional = new double[6][4][4];
        for (int i = 0; i < conditional.length; i++)
            for (int j = 0 ; j < conditional[0].length; j++)
                for (int k = 0; k < conditional[0][0].length; k++)
                    conditional[i][j][k] = 1;
        //System.out.println( CorrectionFunction.correctSingleNucleotideError(conditional, trusted, "ACTACA", 0, 2));
        StringBuilder sb = new StringBuilder().append("AACTTCC");
        CorrectionFunction.correctBorderUntrustedKmers(conditional, trusted, "AACTTCC", 1, 4, sb) ;
        System.out.println(sb.toString());
    }
}

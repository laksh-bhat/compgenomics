package asgmt1;

import java.util.List;

import static fileutils.FileUtils.getStringsFromStdIn;

/**
 * Author   : bhatl
 * Date     : 9/6/13
 * Time     : 2:12 AM
 */
public class HammingDistanceComputer {
    public static void main (String[] args) {
        List<String> strings = getStringsFromStdIn();
        String dnaString     = strings.get(0);
        String mutatedString = strings.get(1);
        assert (dnaString.length() == mutatedString.length());
        System.out.println(computeHammingDistance(dnaString, mutatedString));
    }

    public static int computeHammingDistance (String str1, String str2) {
        if(str1.hashCode() == str2.hashCode())
            return 0;
        int hammingDistance = 0;
        for ( int i = 0 ; i < str1.length() ; i++ ){
            if ( str1.charAt(i) != str2.charAt(i) )
                hammingDistance ++;
        }
        return hammingDistance;
    }
}

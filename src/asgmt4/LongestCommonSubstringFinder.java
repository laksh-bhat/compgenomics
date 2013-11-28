package asgmt4;

import fileutils.FileUtils;
import java.util.*;


/**
 * User: lbhat@damsl
 * Date: 11/9/13
 * Time: 5:00 PM
 */

public class LongestCommonSubstringFinder {

    public static void main (String[] args) {
        List<String> strings    = FileUtils.getStringsFromStdIn();
        List<String> dnaStrings = FileUtils.getNucleotideStringsFromFastaStrings(strings);
        int maxLen = findLongestCommonSubstringLen(dnaStrings.get(0), dnaStrings.get(1));
        Set<String> intersectionGlobal = new HashSet<String>();

        for (int i = 0; i < dnaStrings.size(); i++) {
            // n pairwise comparisions are sufficient. why? All pairs must share at least an lcs.
            // when s is the last string in the collection, we want to compare it with first string.
            final String s = dnaStrings.get(i);
            final String t = dnaStrings.get((i + 1) % dnaStrings.size());
            Set<String> common = getCommonSubStrings(s, t, maxLen);
            if(intersectionGlobal.size() == 0) {intersectionGlobal = common;}
            else intersectionGlobal.retainAll(common); // set intersection
        }
        String maxSubstring = "";
        for (String i : intersectionGlobal)
            if (i.length() > maxSubstring.length())
                maxSubstring = i;

        System.out.println(maxSubstring);
    }

    public static Set<String> getCommonSubStrings(String s, String t, int maxLen){
        Set<String> common = new HashSet<String>();
        Set<String> justS = new HashSet<String>();
        for (int i = 0; i < s.length(); i++){
            for (int l = 1; l <= maxLen && i + l <= s.length(); l++){
                justS.add(s.substring(i, i+l));
            }
        }
        for (int j = 0; j < t.length(); j++){
            for (int l = 1; l <= maxLen && j+l <= t.length(); l++){
                String subString  = t.substring(j, j+l);
                if(justS.contains(subString))
                    common.add(subString);
            }
        }
        return common;
    }

    public static int findLongestCommonSubstringLen (String s, String t) {
        int[][] dpTable = new int[s.length() + 1][t.length() + 1];
        for (int i = 0; i <= s.length(); i++)
            dpTable[i][0] = 0;
        for (int i = 0; i <= t.length(); i++)
            dpTable[0][i] = 0;

        int maxLen = 0;
        for (int i = 0; i < s.length(); i++) {
            for (int j = 0; j < t.length(); j++) {
                if (s.charAt(i) == t.charAt(j)) {
                    dpTable[i + 1][j + 1] = dpTable[i][j] + 1;
                    if (dpTable[i + 1][j + 1] > maxLen)
                        maxLen = dpTable[i + 1][j + 1];
                } else {
                    dpTable[i + 1][j + 1] = 0;
                }
            }
        }
        return maxLen;
    }
}

package asgmt3;

import java.io.*;
import java.text.MessageFormat;
import java.util.*;

/**
 * User: lbhat@damsl
 * Date: 10/16/13
 * Time: 8:19 PM
 */
public class IndexZeroOneTwoMatcher {

    private static String getText(String filename) throws IOException {
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(new File(filename)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder();
        char[] charArray = new char[100];
        while (reader.read(charArray) > 0)
            stringBuilder.append(charArray);

        return stringBuilder.toString();
    }

    public static void main(String[] args) throws IOException {
        String pattern = args[0];
        String text = getText(args[1]);
        int substrLen = 4, patternLength = 12;
        String firstPattern = pattern.substring(0, substrLen);
        String secondPattern = pattern.substring(substrLen, substrLen*2);
        String thirdPattern =  pattern.substring(substrLen*2, pattern.length());

        HashMap<String, List<Integer>> index = buildIndex(substrLen, text);

        List<Integer> hitsForFirstPattern = new ArrayList<Integer>(),
                hitsForSecondPattern = new ArrayList<Integer>()
                        , hitsForThirdPattern = new ArrayList<Integer>();
        if (index.containsKey(firstPattern))
            hitsForFirstPattern = index.get(firstPattern);
        if (index.containsKey(secondPattern))
            hitsForSecondPattern = index.get(secondPattern);
        if (index.containsKey(thirdPattern))
            hitsForThirdPattern = index.get(thirdPattern);

        int noOfExactMatches = 0, noOfOneMismatches = 0, noOfTwoMismatches = 0;
        //Set<Integer> uniqueMatches = new HashSet<Integer>();
        HashMap<Integer, Set<Integer>> matchedSets = new HashMap<Integer, Set<Integer>>();
        for(int i = 0; i <= 2; i++){
            matchedSets.put(i, new HashSet<Integer>());
        }

        for (int hit : hitsForFirstPattern) {
            if (hitsForSecondPattern != null
                    && hitsForSecondPattern.contains(hit + substrLen)
                    && hitsForThirdPattern.contains(hit + 2*substrLen)) {
                // both partitions have a hit
                noOfExactMatches++;
            } else {
                // There was an exact match for 1st partition, so check if there is a 1-mismatch in second partition
                int noOfMismatches = 0;
                for (int i = hit + substrLen, j = 0; j < substrLen && noOfMismatches <= 2; i++, j++) {
                    if (text.charAt(i) != secondPattern.charAt(j))
                        noOfMismatches++;
                }
                for (int i = hit + 2*substrLen, j = 0; j < substrLen && noOfMismatches <= 2; i++, j++) {
                    if (text.charAt(i) != thirdPattern.charAt(j))
                        noOfMismatches++;
                }

                if (noOfMismatches == 1) {
                    if(matchedSets.get(noOfMismatches).add(hit + hit + substrLen +hit + 2*substrLen))
                        noOfOneMismatches++;
                }
                if(noOfMismatches == 2) {
                    if(matchedSets.get(noOfMismatches).add(hit + hit + substrLen +hit + 2*substrLen))
                        noOfTwoMismatches++;
                }
            }
        }

        for (int hit : hitsForSecondPattern) {
            int noOfMismatches = 0;
            // There was an exact match for 2nd partition, so check if there is a 1-mismatch in first partition
            for (int i = hit - substrLen, j = 0; j < substrLen && noOfMismatches <= 2; i++, j++) {
                if (text.charAt(i) != firstPattern.charAt(j))
                    noOfMismatches++;
            }
            for (int i = hit + substrLen, j = 0; j < substrLen && noOfMismatches <= 2; i++, j++) {
                if (text.charAt(i) != thirdPattern.charAt(j))
                    noOfMismatches++;
            }
            if (noOfMismatches == 1) {
                if(matchedSets.get(noOfMismatches).add(hit - substrLen + hit + hit + substrLen)) {
                    noOfOneMismatches++;
                }
            }

            if (noOfMismatches == 2){
                if(matchedSets.get(noOfMismatches).add(hit - substrLen + hit + hit + substrLen)) {
                    noOfTwoMismatches ++;
                }
            }
        }

        for (int hit : hitsForThirdPattern) {
            int noOfMismatches = 0;
            // There was an exact match for 2nd partition, so check if there is a 1-mismatch in first partition
            for (int i = hit - 2*substrLen, j = 0; j < substrLen && noOfMismatches <= 2; i++, j++) {
                if (text.charAt(i) != firstPattern.charAt(j))
                    noOfMismatches++;
            }
            for (int i = hit - substrLen, j = 0; j < substrLen && noOfMismatches <= 2; i++, j++) {
                if (text.charAt(i) != secondPattern.charAt(j))
                    noOfMismatches++;
            }

            if (noOfMismatches == 1) {
                if(matchedSets.get(noOfMismatches).add(hit - 2*substrLen + hit + hit + substrLen))
                    noOfOneMismatches++;
            }

            if (noOfMismatches == 2){
                if(matchedSets.get(noOfMismatches).add(hit - 2*substrLen + hit + hit + substrLen)){
                    noOfTwoMismatches ++;
                }
            }
        }

        System.out.println(MessageFormat.format("Pattern string \t\t\t=> {0}", pattern));
        System.out.println(MessageFormat.format("No. of Exact Matches  => {0}", noOfExactMatches));
        System.out.println(MessageFormat.format("No. of OneMisMatches  => {0}", noOfOneMismatches));
        System.out.println(MessageFormat.format("No. of twoMisMatches  => {0}", noOfTwoMismatches));
        System.out.println(MessageFormat.format("Specificity           => {0}", (noOfExactMatches + noOfOneMismatches + noOfTwoMismatches) * 1.0 /
                (hitsForFirstPattern.size() + hitsForSecondPattern.size() + hitsForThirdPattern.size())));
    }

    private static HashMap<String, List<Integer>> buildIndex(int substringLength, String text) {
        HashMap<String, List<Integer>> index = new HashMap<String, List<Integer>>();
        for (int i = 0; i < text.length() - substringLength + 1; i++) {
            // extract substring
            String substring = text.substring(i, i + substringLength);
            if (!index.containsKey(substring)) index.put(substring, new ArrayList<Integer>());

            // Now add the index at the back of the list
            index.get(substring).add(i);
        }
        return index;
    }

}

package asgmt3;

import fileutils.FileUtils;

import java.io.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

/**
 * User: lbhat@damsl
 * Date: 10/11/13
 * Time: 11:06 PM
 */
public class IndexAssistedZeroOneApproximateMatcher {

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
        String text = getText(args[0]);
        String pattern = args[1];
        int substringLength = 6, patternLength = 12;
        String firstPattern = pattern.substring(0, substringLength);
        String secondPattern = pattern.substring(substringLength, pattern.length());

        HashMap<String, List<Integer>> index = buildIndex(substringLength, text);

        List<Integer> hitsForFirstPattern = new ArrayList<Integer>(), hitsForSecondPattern = new ArrayList<Integer>();
        if (index.containsKey(firstPattern))
            hitsForFirstPattern = index.get(firstPattern);
        if (index.containsKey(secondPattern))
            hitsForSecondPattern = index.get(secondPattern);

        int noOfExactMatches = 0;
        int noOfOneMismatches = 0;

        for (int hit : hitsForFirstPattern) {
            if (hitsForSecondPattern != null && hitsForSecondPattern.contains(hit + substringLength)) {
                // both partitions have a hit
                noOfExactMatches++;
            } else {
                // There was an exact match for 1st partition, so check if there is a 1-mismatch in second partition
                int noOfMismatches = 0;
                for (int textIndex = hit + substringLength, patternIndex = 0;
                     textIndex < hit + 2 * substringLength && noOfMismatches < 2;
                     textIndex++, patternIndex++) {
                    if (text.charAt(textIndex) != secondPattern.charAt(patternIndex))
                        noOfMismatches++;
                }
                if (noOfMismatches == 1)
                    noOfOneMismatches++;
            }
        }

        for (int hit : hitsForSecondPattern) {
            int noOfMismatches = 0;
            // There was an exact match for 2nd partition, so check if there is a 1-mismatch in first partition
            for (int i = hit - substringLength, j = 0; i < hit && noOfMismatches < 2; i++, j++) {
                if (text.charAt(i) != firstPattern.charAt(j))
                    noOfMismatches++;
            }
            if (noOfMismatches == 1)
                noOfOneMismatches++;
        }

        System.out.println(MessageFormat.format("No. of Exact Matches  => {0}", noOfExactMatches));
        System.out.println(MessageFormat.format("No. of OneMisMatches  => {0}", noOfOneMismatches));
        System.out.println(MessageFormat.format("Specificity           => {0}", (noOfExactMatches + noOfOneMismatches)* 1.0 / (hitsForFirstPattern.size() + hitsForSecondPattern.size())));
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

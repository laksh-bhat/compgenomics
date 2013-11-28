package asgmt3;

import asgmt1.HammingDistanceComputer;
import fileutils.FileUtils;

import java.io.*;
import java.text.MessageFormat;
import java.util.*;

/**
 * User: lbhat@damsl
 * Date: 10/11/13
 * Time: 11:06 PM
 */
public class IndexAssistedUptoKApproximateMatcher {

    public static void main(String[] args) throws IOException {

        int noOfExactMatches    = 0;
        String pattern          = args[1];
        String text             = getText(args);
        int patternLength       = pattern.length();
        int substringLength     = Integer.valueOf(args[2]);

        List<String> patterns                    = new ArrayList<String>();
        List<List<Integer>> listOfPartitionHits  = new ArrayList<List<Integer>>();
        HashMap<Integer, Integer> mismatchCounts = new HashMap<Integer, Integer>();

        // generate partitions
        for (int i = 0 ; i < patternLength; i += substringLength)
            patterns.add(pattern.substring(i, i + substringLength));

        HashMap<Integer, Set<Integer>> matchedSets = new HashMap<Integer, Set<Integer>>();
        for(int i = 0; i < pattern.length(); i++){
            mismatchCounts.put(i, 0);
            matchedSets.put(i, new HashSet<Integer>());
        }

        // generate indexes
        HashMap<String, List<Integer>> index = buildIndex(substringLength, text);

        for (int i = 0 ; i < patterns.size(); i++){
            if (index.containsKey(patterns.get(i))) {
                listOfPartitionHits.add(i, index.get(patterns.get(i)));
            }else{
                // This is a hack just to avoid nasty checks later (to avoid NullPointerException)
                listOfPartitionHits.add(i, new ArrayList<Integer>());
            }
        }

        List<Integer> firstPartitionHits = listOfPartitionHits.get(0);
        for (int hit : firstPartitionHits) {
            boolean mismatch = false;
            for (int j = 1; j < listOfPartitionHits.size(); j++) {
                if (!listOfPartitionHits.get(j).contains(hit + j * substringLength)) {
                    mismatch = true;
                    break;
                }
            }
            if (!mismatch) noOfExactMatches++;
        }

        for (int i = 0; i < listOfPartitionHits.size(); i++) {
            List<Integer> indexHitsInPartition = listOfPartitionHits.get(i);
            for (int hit : indexHitsInPartition) {
                int noOfMismatches = 0;
                int hashCode = hit;
                for (int j = 0; j < patterns.size(); j++) {
                    if (i == j) continue;
                    int t = hit + (j - i) * substringLength;
                    String currentPattern = patterns.get(j);
                    String textToVerify = text.substring(t, t+substringLength);
                    noOfMismatches += HammingDistanceComputer.computeHammingDistance(currentPattern, textToVerify);
                    hashCode += t;
                }
                if(noOfMismatches < patterns.size() && matchedSets.get(noOfMismatches).add(hashCode))
                    mismatchCounts.put(noOfMismatches, mismatchCounts.get(noOfMismatches) + 1);
            }
        }

        int totalMatch = noOfExactMatches;
        //normalize mismatch counts
        for (int howManyMismatches : mismatchCounts.keySet() ){
            totalMatch += mismatchCounts.get(howManyMismatches);
        }

        int totalHits = 0;
        for (List<Integer> hitList : listOfPartitionHits)
            totalHits += hitList.size();

        //normalize mismatch counts
        for (int howManyMismatches : mismatchCounts.keySet() )
            mismatchCounts.put(howManyMismatches, mismatchCounts.get(howManyMismatches) );

        System.out.println(MessageFormat.format("Pattern string \t\t\t=> {0}", pattern));
        System.out.println(MessageFormat.format("No. of Exact Matches \t=> {0}", noOfExactMatches));
        for (int howManyMismatches = 0; howManyMismatches < patterns.size(); howManyMismatches++){
            System.out.println();
            System.out.println(MessageFormat.format("No. of {1} mismatches \t=> {0}", mismatchCounts.get(howManyMismatches), howManyMismatches));
            System.out.println(MessageFormat.format("Specificity \t\t\t=> {0}", (noOfExactMatches + totalMatch)* 1.0 / (totalHits), howManyMismatches));
        }
    }

    private static String getText(String[] args) throws IOException {
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(new File(args[0])));
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

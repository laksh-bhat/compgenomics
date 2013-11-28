package asgmt3;

import java.io.*;
import java.text.MessageFormat;
import java.util.*;

/**
 * User: lbhat@damsl
 * Date: 10/12/13
 * Time: 3:02 PM
 */

public class ApproximateSubsequenceMatcher {

    public static void main(String[] args) throws IOException {

        int noOfExactMatches    = 0;
        String text             = getText(args);
        int subsequenceLength   = 6; // bitmap contains 6 1's
        HashMap<String, List<Integer>> index = buildSpaceSeedIndex(subsequenceLength, text);

        for(String pattern : new String[]{"circumstance", "expectations", "handkerchief", "discontented", "performances"}){

            List<String> patterns                    = new ArrayList<String>();
            List<List<Integer>> listOfPartitionHits  = new ArrayList<List<Integer>>();
            HashMap<Integer, Integer> mismatchCounts = new HashMap<Integer, Integer>();

            // generate partitions
            for (int i = 0; i < 2; i++){
                String subsequence = MessageFormat.format("{0}{1}{2}{3}{4}{5}",
                        pattern.charAt(i), pattern.charAt(i + 2), pattern.charAt(i + 4), pattern.charAt(i + 6), pattern.charAt(i + 8), pattern.charAt(i + 10));
                patterns.add(subsequence);
            }

            HashMap<Integer, Set<Integer>> matchedSets = new HashMap<Integer, Set<Integer>>();
            for(int i = 0; i <= patterns.size(); i++) {
                mismatchCounts.put(i, 0);
                matchedSets.put(i, new HashSet<Integer>());
            }

            // generate indexes

            for (int i = 0 ; i < patterns.size(); i++){
                if (index.containsKey(patterns.get(i))) {
                    listOfPartitionHits.add(i, index.get(patterns.get(i)));
                }else{
                    // This is a hack just to avoid nasty checks later (to avoid NullPointerException)
                    listOfPartitionHits.add(i, new ArrayList<Integer>());
                }
            }

            for (int i = 0; i < listOfPartitionHits.size(); i++) {
                List<Integer> indexHitsInPartition = listOfPartitionHits.get(i);
                for (int hit : indexHitsInPartition) {
                    int hashCode = hit;
                    int noOfMismatches = 0;
                    for (int j = 0; j < listOfPartitionHits.size(); j++) {
                        if (i == j) continue;
                        int t = hit + (j - i);
                        hashCode += t;
                        for (int p = 0; p < patterns.get(j).length() && noOfMismatches <= patterns.size(); t += 2, p++) {
                            if (text.charAt(t) != patterns.get(j).charAt(p))
                                noOfMismatches++;
                        }
                    }
                    if (noOfMismatches < patterns.size() && matchedSets.get(noOfMismatches).add(hashCode))
                        mismatchCounts.put(noOfMismatches, mismatchCounts.get(noOfMismatches) + 1);
                }
            }


            int totalMatch = 0;
            for (int howManyMismatches : mismatchCounts.keySet() ){
                totalMatch += mismatchCounts.get(howManyMismatches);
            }

            int totalHits = 0;
            for (List<Integer> hitList : listOfPartitionHits)
                totalHits += hitList.size();

            System.out.println(MessageFormat.format("Pattern string \t\t\t=> {0}", pattern));

            for (int howManyMismatches = 0; howManyMismatches <= mismatchCounts.size(); howManyMismatches++){
                System.out.println(MessageFormat.format("No. of {1} mismatches \t=> {0}", mismatchCounts.get(howManyMismatches), howManyMismatches));
                System.out.println(MessageFormat.format("Specificity \t\t\t=> {0}", totalMatch * 1.0 / (totalHits)));
            }
            System.out.println();
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

    private static HashMap<String, List<Integer>> buildSpaceSeedIndex(int subsequenceLength, String text) {
        HashMap<String, List<Integer>> index = new HashMap<String, List<Integer>>();
        for (int i = 0; i < text.length() - subsequenceLength * 2 + 1; i++) {
            // extract subsequence
            String subsequence = MessageFormat.format("{0}{1}{2}{3}{4}{5}", text.charAt(i), text.charAt(i + 2), text.charAt(i + 4), text.charAt(i + 6), text.charAt(i + 8), text.charAt(i + 10));
            if (!index.containsKey(subsequence)) index.put(subsequence, new ArrayList<Integer>());

            // Now add the index at the back of the list
            index.get(subsequence).add(i);
        }
        return index;
    }
}

package asgmt1;

import java.util.HashMap;
import java.util.Map;

import static fileutils.FileUtils.getStringsFromStdIn;

/**
 * Author   : bhatl
 * Date     : 9/5/13
 * Time     : 6:44 PM
 */
public class DnaNucleotideCounter {
    public static void main (String[] args) {

        String dnaString = getStringsFromStdIn().get(0);
        Map<Character, Integer> nucleotideCounts = new HashMap<Character, Integer>();
        for ( char nucleotide : dnaString.toCharArray() )
            if(nucleotideCounts.containsKey(nucleotide))
                nucleotideCounts.put(nucleotide, nucleotideCounts.get(nucleotide) + 1);
            else
                nucleotideCounts.put(nucleotide, 1);
        printACGTCounts(nucleotideCounts);
    }

    private static void printACGTCounts (Map<Character, Integer> nucleotideCounts) {
        char charArr[] = {'A', 'C', 'G', 'T'};
        for ( char character : charArr )
        {
            System.out.print(nucleotideCounts.containsKey(character) ? nucleotideCounts.get(character) : 0);
            System.out.print(' ');
        }
    }
}

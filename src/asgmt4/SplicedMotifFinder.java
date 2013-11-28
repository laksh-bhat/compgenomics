package asgmt4;

import fileutils.FileUtils;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * User: lbhat@damsl
 * Date: 11/10/13
 * Time: 3:45 PM
 */
public class SplicedMotifFinder {
    public static void main (String[] args) {
        List<String> strings = FileUtils.getStringsFromStdIn();
        List<String> sAndT = FileUtils.getNucleotideStringsFromFastaStrings(strings);
        swapIf0GreaterThan1(sAndT);
        List<Integer> indices = findMotif(sAndT.get(0), sAndT.get(1));
        for (int index : indices) {
            System.out.print(MessageFormat.format("{0} ", index + 1));
        }
    }

    public static List<Integer> findMotif (String s, String t) {
        List<Integer> indices = new ArrayList<Integer>();
        int j = 0;
        for (int i = 0; i < t.length(); i++) {
            while (j < s.length()){
                if (t.charAt(i) == s.charAt(j)) {
                    indices.add(j++);
                    break;
                } else j++;
            }
        }
        return indices;
    }

    public static void swapIf0GreaterThan1 (List<String> strings) {
        if (strings.get(0).length() > strings.get(1).length())
            return;
        String temp = strings.get(1);
        strings.add(1, strings.get(0));
        strings.add(0, temp);
    }
}

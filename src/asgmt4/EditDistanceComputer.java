package asgmt4;

import fileutils.FileUtils;

import java.util.List;

/**
 * User: lbhat@damsl
 * Date: 11/10/13
 * Time: 4:34 PM
 */
public class EditDistanceComputer {

    public static void main (String[] args) {
        List<String> strings = FileUtils.getStringsFromStdIn();
        strings = FileUtils.getNucleotideStringsFromFastaStrings(strings);
        System.out.println(computeEditDistance(strings.get(0), strings.get(1)));
    }
    public static int minimum (int a, int b, int c) {
        return Math.min(Math.min(a, b), c);
    }

    public static int computeEditDistance (String string1, String string2) {
        int[][] distance = new int[string1.length() + 1][string2.length() + 1];

        for (int i = 0; i <= string1.length(); i++)
            distance[i][0] = i;
        for (int j = 1; j <= string2.length(); j++)
            distance[0][j] = j;

        for (int i = 1; i <= string1.length(); i++)
            for (int j = 1; j <= string2.length(); j++)
                distance[i][j] = minimum(
                        distance[i - 1][j] + 1,
                        distance[i][j - 1] + 1,
                        distance[i - 1][j - 1]
                                + ((string1.charAt(i - 1) == string2.charAt(j - 1)) ? 0
                                : 1));

        return distance[string1.length()][string2.length()];
    }
}

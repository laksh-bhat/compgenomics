package asgmt1;

import static fileutils.FileUtils.getStringsFromStdIn;

/**
 * Author   : bhatl
 * Date     : 9/5/13
 * Time     : 10:05 PM
 */
public class RnaTranscriber {
    public static void main (String[] args) {
        String dnaString = getStringsFromStdIn().get(0);
        System.out.println(transcribe(dnaString));
    }

    public static String transcribe (String dnaString) {
        dnaString = dnaString.toUpperCase();
        return dnaString.replace('T', 'U');
    }
}

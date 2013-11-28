package asgmt1;

import java.util.ArrayList;
import java.util.List;

import static fileutils.FileUtils.getStringsFromStdIn;

/**
 * Author   : bhatl
 * Date     : 9/6/13
 * Time     : 4:11 PM
 */
public class SubstringPositionFinder {
    public static void main (String[] args) {
        List<String> strings = getStringsFromStdIn();
        String mainStr = strings.get(0).length() > strings.get(1).length()? strings.get(0) : strings.get(1);
        String subStr  = strings.get(0).length() < strings.get(1).length()? strings.get(0) : strings.get(1);

        List<Integer> substringIndices = new ArrayList<Integer>();
        while ( true ) {
            int previousSubStrMatchIndex = substringIndices.size() > 0? substringIndices.get(substringIndices.size() - 1) : 0;
            int currentSubStrMatchIndex = mainStr.indexOf(subStr, previousSubStrMatchIndex);
            if(currentSubStrMatchIndex > -1 ) /*substring found*/
                substringIndices.add(currentSubStrMatchIndex + 1); // 1-based indexing not java-like 0-based.
            else break;
        }
        for ( Integer position : substringIndices ){
            System.out.print(position);
            System.out.print(" ");
        }
    }
}

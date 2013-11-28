package asgmt1;

import java.util.Stack;

import static fileutils.FileUtils.getStringsFromStdIn;

/**
 * Author   : bhatl
 * Date     : 9/6/13
 * Time     : 1:49 AM
 */
public class DnaReverseComplimenter {
    public static void main (String[] args) {
        String dnaString = getStringsFromStdIn().get(0);
        System.out.println(reverseCompliment(dnaString));
    }

    public static String reverseCompliment (String dnaString) {
        dnaString = dnaString.toUpperCase();
        Stack<Character> reverseCompliment = new Stack<Character>();
        for ( char nt : dnaString.toCharArray() ){
            switch ( nt ) {
                case 'A':
                    nt = 'T';
                    break;
                case 'T':
                    nt = 'A';
                    break;
                case 'G':
                    nt = 'C';
                    break;
                case 'C':
                    nt = 'G';
                    break;
                default:
                    System.out.println("Something is wrong!");
            }
            reverseCompliment.push(nt);
        }
        StringBuilder reverseComplimentDnaStr = new StringBuilder();
        while(!reverseCompliment.isEmpty()){
            reverseComplimentDnaStr.append(reverseCompliment.pop());
        }
        return reverseComplimentDnaStr.toString();
    }
}

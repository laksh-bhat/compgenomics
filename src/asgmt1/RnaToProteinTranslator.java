package asgmt1;

import java.util.HashMap;
import java.util.Map;

import static fileutils.FileUtils.getStringsFromStdIn;

/**
 * Author   : bhatl
 * Date     : 9/6/13
 * Time     : 12:55 PM
 */
public class RnaToProteinTranslator {
    private static Map<String, Character> codonMap;
    static {
        codonMap = new HashMap<String, Character>();
        codonMap.put("UUU", 'F');
        codonMap.put("CUU", 'L');
        codonMap.put("AUU", 'I');
        codonMap.put("GUU", 'V');
        codonMap.put("UUC", 'F');
        codonMap.put("CUC", 'L');
        codonMap.put("AUC", 'I');
        codonMap.put("GUC", 'V');
        codonMap.put("UUA", 'L');
        codonMap.put("CUA", 'L');
        codonMap.put("AUA", 'I');
        codonMap.put("GUA", 'V');
        codonMap.put("UUG", 'L');
        codonMap.put("CUG", 'L');
        codonMap.put("AUG", 'M');
        codonMap.put("GUG", 'V');
        codonMap.put("UCU", 'S');
        codonMap.put("CCU", 'P');
        codonMap.put("ACU", 'T');
        codonMap.put("GCU", 'A');
        codonMap.put("UCC", 'S');
        codonMap.put("CCC", 'P');
        codonMap.put("ACC", 'T');
        codonMap.put("GCC", 'A');
        codonMap.put("UCA", 'S');
        codonMap.put("CCA", 'P');
        codonMap.put("ACA", 'T');
        codonMap.put("GCA", 'A');
        codonMap.put("UCG", 'S');
        codonMap.put("CCG", 'P');
        codonMap.put("ACG", 'T');
        codonMap.put("GCG", 'A');
        codonMap.put("UAU", 'Y');
        codonMap.put("CAU", 'H');
        codonMap.put("AAU", 'N');
        codonMap.put("GAU", 'D');
        codonMap.put("UAC", 'Y');
        codonMap.put("CAC", 'H');
        codonMap.put("AAC", 'N');
        codonMap.put("GAC", 'D');
        codonMap.put("UAA", '!');
        codonMap.put("CAA", 'Q');
        codonMap.put("AAA", 'K');
        codonMap.put("GAA", 'E');
        codonMap.put("UAG", '!');
        codonMap.put("CAG", 'Q');
        codonMap.put("AAG", 'K');
        codonMap.put("GAG", 'E');
        codonMap.put("UGU", 'C');
        codonMap.put("CGU", 'R');
        codonMap.put("AGU", 'S');
        codonMap.put("GGU", 'G');
        codonMap.put("UGC", 'C');
        codonMap.put("CGC", 'R');
        codonMap.put("AGC", 'S');
        codonMap.put("GGC", 'G');
        codonMap.put("UGA", '!');
        codonMap.put("CGA", 'R');
        codonMap.put("AGA", 'R');
        codonMap.put("GGA", 'G');
        codonMap.put("UGG", 'W');
        codonMap.put("CGG", 'R');
        codonMap.put("AGG", 'R');
        codonMap.put("GGG", 'G');
    }

    public static void main (String[] args) {
        String rna = getStringsFromStdIn().get(0);
        String protein = translateRnaStringToProtein(rna);
        System.out.println(protein);
    }

    public static String translateRnaStringToProtein (String rna) {
       StringBuilder protein = new StringBuilder();
       for(int i = 0; i + 2 < rna.length(); i = i + 3){
           StringBuilder codon = new StringBuilder();
           codon.append(rna.charAt(i)).append(rna.charAt(i + 1)).append(rna.charAt(i + 2));
           assert (codonMap.containsKey(codon.toString()));
           if(codonMap.get(codon.toString()) == '!')
               break;
           protein.append(codonMap.get(codon.toString()));
       }
        return protein.toString();
    }
}

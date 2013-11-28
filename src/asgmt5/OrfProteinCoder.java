package asgmt5;

import asgmt1.DnaReverseComplimenter;
import asgmt1.RnaToProteinTranslator;
import asgmt1.RnaTranscriber;
import fileutils.FileUtils;
import java.util.*;

/**
 * User: lbhat@damsl
 * Date: 11/16/13
 * Time: 7:05 PM
 */
public class OrfProteinCoder {
    public static final String startCodon = "ATG";
    public static final String[] stopCodons = {"TAG", "TGA", "TAA"};

    public static void main (String[] args) {
        String dna = FileUtils.getNucleotideStringsFromFastaStrings(FileUtils.getStringsFromStdIn()).get(0);
        HashSet<String> proteins = new HashSet<String>();
        getPossibleProteinStrings(dna, proteins);
        getPossibleProteinStrings(DnaReverseComplimenter.reverseCompliment(dna), proteins);
        for (String protein : proteins)
            System.out.println(protein);
    }

    private static void getPossibleProteinStrings (final String dna, final HashSet<String> proteins) {
        for (int i = 0; i < 3 && dna.length() > i; i++) {
            int currentStartCodonIndex = -1;
            String shiftedDna = dna.substring(i);
            while ((currentStartCodonIndex = shiftedDna.indexOf(startCodon, currentStartCodonIndex + 1)) > 0) {
                int times = 0;
                for (String stopCodon : stopCodons)
                    if (shiftedDna.lastIndexOf(stopCodon) <= currentStartCodonIndex)
                        times++;
                if (times == stopCodons.length)
                    return;
                String rna = RnaTranscriber.transcribe(shiftedDna.substring(currentStartCodonIndex));
                String protein = RnaToProteinTranslator.translateRnaStringToProtein(rna);
                proteins.add(protein);
            }
        }
    }
}

package asgmt3;

import asgmt1.RnaToProteinTranslator;
import asgmt1.RnaTranscriber;

import java.util.ArrayList;
import java.util.List;

import static fileutils.FileUtils.getAllStringsFromFile;
import static fileutils.FileUtils.getStringsFromStdIn;

/**
 * User: lbhat@damsl
 * Date: 10/6/13
 * Time: 11:19 AM
 */
public class RnaSplicer {
    public static void main(String[] args) {
        List<String> strings        = getStringsFromStdIn();
        List<String> splicingInputs = getNucleotideStringsFromFastaStrings(strings);
        String dna = splicingInputs.get(0);
        for (int i = 1; i < splicingInputs.size(); i++){
            dna = dna.replaceAll(splicingInputs.get(i), "");
        }
        String mRna = RnaTranscriber.transcribe(dna);
        System.out.println(RnaToProteinTranslator.translateRnaStringToProtein(mRna));
    }

    public static List<String> getNucleotideStringsFromFastaStrings(List<String> strings) {
        List<String> splicingInputs = new ArrayList<String>();
        int i = 0;
        while(true){
            String input = "", label = strings.get(i++);
            while(i < strings.size() && strings.get( i ).charAt(0) != '>' ){
                input += strings.get(i++);
            }
            splicingInputs.add(input);

            if(i >= strings.size()) break;
        }
        return splicingInputs;
    }
}

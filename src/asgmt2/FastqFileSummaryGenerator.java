package asgmt2;

import fileutils.FileUtils;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;

/**
 * Author   : bhatl
 * Date     : 9/27/13
 * Time     : 1:10 AM
 */


public class FastqFileSummaryGenerator {
    public FastqFileSummaryGenerator () {
        aNucleotidesPositionMap = new HashMap<Integer, Integer>();
        cNucleotidesPositionMap = new HashMap<Integer, Integer>();
        tNucleotidesPositionMap = new HashMap<Integer, Integer>();
        gNucleotidesPositionMap = new HashMap<Integer, Integer>();
        otherCharactersPositionMap = new HashMap<Integer, Integer>();

        phredScaledQualityLessThan20 = new HashMap<Integer, Integer>();
        phredScaledQualitygreaterThan20 = new HashMap<Integer, Integer>();
    }

    public static void main (String[] args) {
        FastqFileSummaryGenerator fastqFileSummaryGenerator = new FastqFileSummaryGenerator();
        List<String> fastqReads = FileUtils.getAllStringsFromFile("G:\\code\\compgenomics\\res\\asgmt2\\hq1_reads.fastq"/*args[0]*/);

        for (int i = 0; i + 3 < fastqReads.size(); i += 4) {
            String nucleotides = fastqReads.get(i + 1);
            String phredScores = fastqReads.get(i + 3);

            for (int j = 0; j < nucleotides.length(); j++) {
                switch (nucleotides.charAt(j)) {
                    case 'A':
                        updateArbitraryMap(j, fastqFileSummaryGenerator.aNucleotidesPositionMap);
                        break;
                    case 'C':
                        updateArbitraryMap(j, fastqFileSummaryGenerator.cNucleotidesPositionMap);
                        break;
                    case 'T':
                        updateArbitraryMap(j, fastqFileSummaryGenerator.tNucleotidesPositionMap);
                        break;
                    case 'G':
                        updateArbitraryMap(j, fastqFileSummaryGenerator.gNucleotidesPositionMap);
                        break;
                    default:
                        updateArbitraryMap(j, fastqFileSummaryGenerator.otherCharactersPositionMap);
                }

                System.out.println(1.0 - Math.pow(10.0, -0.1 * (phredScores.charAt(j) - 33)));
                if (phredScores.charAt(j) - 33 >= 20) {
                    updateArbitraryMap(j, fastqFileSummaryGenerator.phredScaledQualitygreaterThan20);
                } else {
                    updateArbitraryMap(j, fastqFileSummaryGenerator.phredScaledQualityLessThan20);
                }
            }
        }

        for (int position = 0; position < fastqFileSummaryGenerator.aNucleotidesPositionMap.keySet().size(); position++) {
            System.out.println(
                    MessageFormat.format(
                            "{0} {1} {2} {3} {4} {5} {6}",
                            fastqFileSummaryGenerator.aNucleotidesPositionMap.containsKey(position) ?
                                    fastqFileSummaryGenerator.aNucleotidesPositionMap.get(position) : 0,
                            fastqFileSummaryGenerator.cNucleotidesPositionMap.containsKey(position) ?
                                    fastqFileSummaryGenerator.cNucleotidesPositionMap.get(position) : 0,
                            fastqFileSummaryGenerator.gNucleotidesPositionMap.containsKey(position) ?
                                    fastqFileSummaryGenerator.gNucleotidesPositionMap.get(position) : 0,
                            fastqFileSummaryGenerator.tNucleotidesPositionMap.containsKey(position) ?
                                    fastqFileSummaryGenerator.tNucleotidesPositionMap.get(position) : 0,
                            fastqFileSummaryGenerator.otherCharactersPositionMap.containsKey(position) ?
                                    fastqFileSummaryGenerator.otherCharactersPositionMap.get(position) : 0,
                            fastqFileSummaryGenerator.phredScaledQualityLessThan20.containsKey(position) ?
                                    fastqFileSummaryGenerator.phredScaledQualityLessThan20.get(position) : 0,
                            fastqFileSummaryGenerator.phredScaledQualitygreaterThan20.containsKey(position) ?
                                    fastqFileSummaryGenerator.phredScaledQualitygreaterThan20.get(position) : 0
                    )
            );
        }
    }

    private static void updateArbitraryMap (int key, HashMap<Integer, Integer> map) {
        if (map.containsKey(key)) {
            map.put(key, map.get(key) + 1);
        } else {
            map.put(key, 1);
        }
    }

    private HashMap<Integer, Integer> aNucleotidesPositionMap;
    private HashMap<Integer, Integer> cNucleotidesPositionMap;
    private HashMap<Integer, Integer> gNucleotidesPositionMap;
    private HashMap<Integer, Integer> tNucleotidesPositionMap;
    private HashMap<Integer, Integer> otherCharactersPositionMap;

    private HashMap<Integer, Integer> phredScaledQualityLessThan20;
    private HashMap<Integer, Integer> phredScaledQualitygreaterThan20;
}

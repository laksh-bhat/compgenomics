package asgmt1;

import java.text.MessageFormat;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import static fileutils.FileUtils.getStringsFromStdIn;

/**
 * Author   : bhatl
 * Date     : 9/6/13
 * Time     : 5:27 PM
 */
public class GcContentComputer {
    public static void main (String[] args) {
        SortedMap<Double, String> gcContentMap = new TreeMap<Double, String>();
        List<String> strings = getStringsFromStdIn();
        int i = 0;
        while(true){
            String dna = "", label = strings.get(i++);
            while(i < strings.size() && strings.get( i ).charAt(0) != '>' ){
                dna += strings.get(i++);
            }
            gcContentMap.put(getGcContent(dna), label.substring(1));
            if(i >= strings.size()) break;
        }
        System.out.println(
            MessageFormat.format(
                "{0}\n{1}",
                gcContentMap.get(gcContentMap.lastKey()),
                String.format("%.6f", gcContentMap.lastKey())
            )
        );
    }

    private static Double getGcContent (String dna) {
        double gcCount = 0;
        dna = dna.toUpperCase();
        for ( Character ch : dna.toCharArray() ){
            if(ch == 'G' || ch == 'C')
                gcCount++;
        }
        return gcCount/dna.length() * 100;
    }
}

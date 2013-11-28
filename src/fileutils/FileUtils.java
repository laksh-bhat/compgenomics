package fileutils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Author   : bhatl
 * Date     : 9/12/13
 * Time     : 10:18 PM
 */
public class FileUtils {
    public static String getStringFromFile (String fileName) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(fileName));
            return br.readLine();

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            assert br != null;
            try {
                br.close();
            } catch (IOException ignore) {}
        }
        return null;
    }

    public static List<String> getAllStringsFromFile (String fileName) {
        BufferedReader br = null;
        List<String> strings = new ArrayList<String>();
        try {
            br = new BufferedReader(new FileReader(fileName));
            String lineInFile;
            while((lineInFile = br.readLine()) != null) {
                strings.add(lineInFile);
            }
            return strings;

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            assert br != null;
            try {
                br.close();
            } catch (IOException ignore) {}
        }
        return null;
    }

    public static List<String> getStringsFromStdIn(){
        List<String> strings = new ArrayList<String>();
        Scanner sc = new Scanner(System.in);
        while(sc.hasNext()){
            strings.add(sc.nextLine());
        }
        sc.close();
        return strings;
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

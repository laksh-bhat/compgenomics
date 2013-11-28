package fileutils;

import java.io.*;

/**
 * Author   : bhatl
 * Date     : 9/21/13
 * Time     : 1:43 AM
 */
public class csvCleaner {
    public static void main (String[] args) {
        String fileName = args[0];
        String fileName2 = args[1];
        BufferedReader br;
        BufferedWriter bw;
        try {
            br = new BufferedReader(new FileReader(fileName));
            bw = new BufferedWriter(new FileWriter(fileName2));

            String newline;

            String actualLine = "";
            boolean previousLineEndedWithQuote = false;
            while ( (newline = br.readLine()) != null ){

                newline = newline.trim();
                if(newline.startsWith("\"") && !newline.startsWith("\"\"")  && !newline.startsWith("\",") && previousLineEndedWithQuote) {
                    previousLineEndedWithQuote = false;
                    bw.write(actualLine);
                    bw.newLine();
                    actualLine = "";
                }

                if(!newline.endsWith("\"")){
                    actualLine += newline;
                } else if (newline.endsWith("\"")){
                    previousLineEndedWithQuote = true;
                    actualLine += newline;
                }
            }
            bw.close();
            br.close();
        }catch (IOException e){}
    }
}

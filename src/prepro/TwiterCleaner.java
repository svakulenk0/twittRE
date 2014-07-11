package prepro;


import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Scanner;

public class TwiterCleaner {

    public static void main(String[] args) {
        String profileDirectory = "profiles";
        String sentence = "";
        FileInputStream fis;
        Scanner s;

        try {
            DetectorFactory.loadProfile(profileDirectory);
            fis = new FileInputStream("file_1.txt");
            s = new Scanner(fis, "UTF-8");
            while (s.hasNext()) {
                sentence = sentence + " " + s.nextLine();
            }
            s.close();
        } catch (FileNotFoundException ex) {
            System.out.println(ex);
        
	    } catch ( LangDetectException ex) {
	        System.out.println(ex);
	    }
       
        System.out.print(sentence + "\n");
                
        CleanTheTweet clean = new CleanTheTweet(sentence); //Enable to use CleanTheTweet
        System.out.print(clean.getCleanedSentences() + "\n");

        PrintWriter pw;
        try {
            pw = new PrintWriter("out.txt", "UTF-8");
            for(String tweet : clean.getCleanedSentences()){
                pw.print(tweet + "\n");
            }
            
            pw.close();
        } catch (FileNotFoundException ex) {
            System.out.println(ex);
        
	    } catch ( UnsupportedEncodingException ex) {
	        System.out.println(ex);
	    }
    }
}

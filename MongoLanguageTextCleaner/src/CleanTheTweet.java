
import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringEscapeUtils;

public class CleanTheTweet {

    private List<String> ready = new ArrayList<>();
    private String originalSentence = "";
    private String[] tokenizedSentence;
    private String cleanSentence = "";

    /*
     *Construct the class with the original string as parameter.
     *Use the getCleanSentence() to take the prossesed String.
     */
    public CleanTheTweet(String sentence) {
        originalSentence = sentence;
        //System.out.println("Cleaning the Tweet (inside)...");
        removeHTML();
        removeNonEnglish();
        tokenizeSentence();
        cleanIt();
        rebuildIt();
    }

    public List<String> getCleanedSentences() {
        return ready;
    }

    public void printArray() {
        for (int i = 0; i < ready.size(); i++) {
            System.out.println(ready.get(i));
        }
    }

    private void removeNonEnglish() {
        try {
            Detector detector = DetectorFactory.create();
            detector.append(originalSentence);
            String lang = detector.detect();

            if (lang.equals("en")) {
                originalSentence = originalSentence;
            } else {
                originalSentence = "";
            }
        } catch (LangDetectException ex) {
            //System.out.println("Could not detect language! " + ex);
        }
    }

    private void removeHTML() {
        originalSentence = originalSentence.replaceAll("&amp;", "and");
        originalSentence = StringEscapeUtils.unescapeHtml4(originalSentence);

    }

    private void tokenizeSentence() {
        originalSentence = originalSentence.replaceAll("!+", "!");
        originalSentence = originalSentence.replaceAll("\\?+", "?");
        originalSentence = originalSentence.replaceAll("…|\\.+", ".");
        originalSentence = originalSentence.replaceAll("(!\\?)+", "!?");
        originalSentence = originalSentence.replaceAll("(\\?!)+", "?!");
        originalSentence = originalSentence.replaceAll("\\r|\\n|\\r\\n|-", " ");
        originalSentence = originalSentence.replaceAll("\"|#|^|\\*|\\(|\\)|<|>|~|`|[^\\p{ASCII}]", "");
        tokenizedSentence = originalSentence.split("\\s+");
    }

    private void cleanIt() {
        for (int i = 0; i < tokenizedSentence.length; i++) {
            if (tokenizedSentence[i].equals("RT")) {
                tokenizedSentence[i] = "";               
            } else if (tokenizedSentence[i].startsWith("@")) {
                tokenizedSentence[i] = "";
            } else if (tokenizedSentence[i].startsWith("!") || tokenizedSentence[i].startsWith("?")) {
                tokenizedSentence[i] = tokenizedSentence[i].replaceAll("!+", "");
                tokenizedSentence[i] = tokenizedSentence[i].replaceAll("\\?+", "");
            } else if (tokenizedSentence[i].matches("(http)s?://.*") || tokenizedSentence[i].matches("\\W*")) {//Is Link or non-word string  
                if (tokenizedSentence[i].endsWith(".")) {
                    tokenizedSentence[i] = ".";
                } else {
                    tokenizedSentence[i] = "";
                }
            }
        }
    }

    private void rebuildIt() {
        for (int i = 0; i < tokenizedSentence.length; i++) {
            if (!tokenizedSentence[i].equals("")) {
                cleanSentence += tokenizedSentence[i].trim() + " ";
                if (tokenizedSentence[i].endsWith(".") | tokenizedSentence[i].endsWith("!") | tokenizedSentence[i].endsWith("?")) {
                    ready.add(cleanSentence.trim());
                    cleanSentence = "";
                }
            }
        }
        ready.add(cleanSentence.trim());//Adding last sentence
        for (int i = 0; i < ready.size(); i++) {
            if (ready.get(i).length() < 1) {
                ready.remove(i);
            }
        }
    }
}

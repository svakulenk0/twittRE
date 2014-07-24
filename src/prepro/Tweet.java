package prepro;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bson.BasicBSONObject;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import de.mpii.clausie.ClausIE;
import de.mpii.clausie.Proposition;
import prepro.CleanTheTweet;


public class Tweet {
	public List<String> cleanedText = new ArrayList<String>();
	public String ID;
	public Map<String, List<String>> NERs;
	public String tweet_text;
	public String lang = "n_d";
	TwoMaps maps = new TwoMaps();
	DBObject old_temp;
	
	public Tweet (DBObject tweetObject, NameThatEntity nte) {
//		System.out.println("got new");
		this.old_temp = tweetObject;
		this.tweet_text = tweetObject.get("text").toString();
//		CleanTheTweet clt = new CleanTheTweet(tweet_text);
//		this.cleanedText = clt.getCleanedSentences();
		
		// skip language detection!
//		this.lang = tweetObject.get("language").toString();
				//detectLanguage(this.tweet_text);
        
        // we work only with English language texts
//        if (this.lang.equals("en")) {
        	//System.out.println("Cleaning the Tweet (outside)...");
            CleanTheTweet ctt = new CleanTheTweet(this.tweet_text);
            this.cleanedText = ctt.getCleanedSentences();
            //System.out.print("The cleaned sentences are: ");
            //ctt.printArray();
            
            this.maps = nte.tagIt(this.cleanedText);//Standford Tagging!
            this.NERs = maps.nerMap;
//            System.out.println(this.NERs);
//            extractRelations(tweet, clausIE);
//        }
		
		//(List<String>) tweetObject.get("cleaned_sentences");
		this.ID = tweetObject.get("id_str").toString();
//		JSONObject obj=(JSONObject) JSONValue.parse(tweetObject.get("clustering_ner").toString());
//		this.NERs = (Map)obj;
//		System.out.println(NERs);
	}
	
	public String detectLanguage(String text) {
        try {
            Detector detector = DetectorFactory.create();
            detector.append(text);
            lang = detector.detect();
            //System.out.println("Detected language is: " + lang);
        } catch (LangDetectException ex) {
            System.out.println("Could not detect language! " + ex);
//            System.out.println(text);
        }
        return lang;
    }
	
	public void updateObject(DBCollection coll) {
        //String updated = "done: ";
        BasicDBObject new_temp = new BasicDBObject();//Create a new empty object.
        new_temp.putAll(old_temp);//Copying the temp into it.
        
//        if (new_temp.containsKey("language")) {
//            new_temp.removeField("language");
//            new_temp.append("language", lang);//Adding the new key = language!
//            //updated += " 1.1";
//        } else {
//            new_temp.append("language", lang);
//            //updated += " 1.2";
//        }
        
        if (new_temp.containsKey("cleaned_sentences")){
            new_temp.removeField("cleaned_sentences");
            //Adding the Cleaned Sentences.
            //updated += " 2.1";
        }
        
        new_temp.append("cleaned_sentences", this.cleanedText);
        
        if(new_temp.containsKey("clustering")){
            new_temp.removeField("clustering");
        }
        if(new_temp.containsKey("Clusters")){
            new_temp.removeField("Clusters");
        }
        
        if (new_temp.containsKey("clustering_ner")){
            new_temp.removeField("clustering_ner");
            //updated += " 3.1";
        }
            new_temp.append("clustering_ner", this.maps.nerMap);
            //updated += " 3.2";
        
        if (new_temp.containsKey("clustering_pos")){
            new_temp.removeField("clustering_pos");
            //updated += " 3.1";
        }
        new_temp.append("clustering_pos", this.maps.posMap);//Adding the pos map
                                                         
        //System.out.println("The language we added is: " + new_temp.get("language").toString());
        //System.out.println("The cleaned sentences we added are: " + new_temp.get("cleaned_sentences").toString());

        coll.update(old_temp, new_temp);

        //System.out.println("The update was " + updated + ".\n==========");
    }
	
}

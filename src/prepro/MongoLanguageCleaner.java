package prepro;


import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MongoLanguageCleaner {
      
    private DBCollection coll;
    private Mongo mongoClient;
    private NameThatEntity nte;
    private int updatedTweets = 0;
    private String dbname = "tweets";
    private String profileDirectory = "profiles";
    private String mongo_collection = "snow"; // tweets, facup, snow
    private List<DBCollection> collections = new ArrayList<DBCollection>();

    public void StartCon() {
        try {
            nte = new NameThatEntity();
            DetectorFactory.loadProfile(profileDirectory);            
            mongoClient = new Mongo("rs210522.rs.hosteurope.de", 27017);
            coll = mongoClient.getDB(dbname).getCollection(mongo_collection);

        } catch (IOException ex) {
            System.out.println("Could Not Connect to the Database. " + ex);
        } catch (LangDetectException ex) {
            System.out.println("Language profiles are not in the correct folder!. " + ex);
        }
    }

    public void preProcess() {
        String text = "";
        String lang = "";
        List<String> sentences;
        TwoMaps maps;

        BasicDBObject query = new BasicDBObject("clustering_pos", new BasicDBObject("$exists", false)).append("language", "en"); //new BasicDBObject("text", new BasicDBObject("$exists", true));
        DBCursor cursor = coll.find(query);

        while (cursor.hasNext()) {
            DBObject old_temp = cursor.next();//Need to keep this one so we can update later.
            text = old_temp.get("text").toString();
//            System.out.println("The raw text is: " + text);

//            lang = detectLanguage(text);

            //System.out.println("Cleaning the Tweet (outside)...");
            CleanTheTweet ctt = new CleanTheTweet(text);
            sentences = ctt.getCleanedSentences();
            //System.out.print("The cleaned sentences are: ");
            //ctt.printArray();
            //System.out.println("The Cleaning is done!");           
            maps = nte.tagIt(sentences);//Standford Tagging!
            
            updateObject(old_temp, lang, sentences, maps);
        }
        cursor.close();
        mongoClient.close();
        System.out.println(updatedTweets + " tweets where updated!");
    }

    public String detectLanguage(String text) {
        String lang = "n_d";
        try {
            Detector detector = DetectorFactory.create();
            detector.append(text);
            lang = detector.detect();
            //System.out.println("Detected language is: " + lang);
        } catch (LangDetectException ex) {
            System.out.println("Could not detect language! " + ex);
        }
        return lang;
    }

    public void updateObject(DBObject old_temp, String lang, List<String> sentences, TwoMaps maps) {
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
//        
        if (new_temp.containsKey("cleaned_sentences")){
            new_temp.removeField("cleaned_sentences");
            new_temp.append("cleaned_sentences", sentences);//Adding the Cleaned Sentences.
            //updated += " 2.1";
        } else {
            new_temp.append("cleaned_sentences", sentences);
            //updated += " 2.2";
        }
        
        if(new_temp.containsKey("clustering")){
            new_temp.removeField("clustering");
        }
        if(new_temp.containsKey("Clusters")){
            new_temp.removeField("Clusters");
        }
        
        if (new_temp.containsKey("clustering_ner")){
            new_temp.removeField("clustering_ner");
            new_temp.append("clustering_ner", maps.nerMap);//Adding the NER map
            //updated += " 3.1";
        } else {
            new_temp.append("clustering_ner", maps.nerMap);
            //updated += " 3.2";
        }
        
        if (new_temp.containsKey("clustering_pos")){
            new_temp.removeField("clustering_pos");
            new_temp.append("clustering_pos", maps.posMap);//Adding the pos map
            //updated += " 3.1";
        } else {
            new_temp.append("clustering_pos", maps.posMap);
            //updated += " 3.2";
        }
                                                         
        //System.out.println("The language we added is: " + new_temp.get("language").toString());
        //System.out.println("The cleaned sentences we added are: " + new_temp.get("cleaned_sentences").toString());

        coll.update(old_temp, new_temp);
        updatedTweets++;

        //System.out.println("The update was " + updated + ".\n==========");
    }
}

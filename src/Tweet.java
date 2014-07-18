import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bson.BasicBSONObject;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.mongodb.DBObject;
import prepro.CleanTheTweet;


public class Tweet {
	public List<String> cleanedText;
	public String ID;
	public Map<String, List<String>> NERs;
	
	public Tweet (DBObject tweetObject) {
//		String tweet_text = tweetObject.get("text").toString();
//		CleanTheTweet clt = new CleanTheTweet(tweet_text);
//		this.cleanedText = clt.getCleanedSentences();
		this.cleanedText = (List<String>) tweetObject.get("cleaned_sentences");
		this.ID = tweetObject.get("id_str").toString();
		JSONObject obj=(JSONObject) JSONValue.parse(tweetObject.get("clustering_ner").toString());
		this.NERs = (Map)obj;
//		System.out.println(NERs);
	}
}

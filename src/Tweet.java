import java.util.List;
import com.mongodb.DBObject;
import prepro.CleanTheTweet;


public class Tweet {
	public List<String> cleanedText;
	public String ID;
	
	public Tweet (DBObject tweetObject) {
		String tweet_text = tweetObject.get("text").toString();
		CleanTheTweet clt = new CleanTheTweet(tweet_text);
		this.cleanedText = clt.getCleanedSentences();
		this.ID = tweetObject.get("id_str").toString();;
	}
}

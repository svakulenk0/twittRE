import java.io.IOException;
import com.mongodb.Mongo;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;

import de.mpii.clausie.ClausIE;
import de.mpii.clausie.Clause;
import de.mpii.clausie.Proposition;

public class mongoExample {
	public static void main(String[] args) throws IOException {
		
		// connect to MongoDB tweet collection
		Mongo mongo = new Mongo("localhost", 27017);
		DBCollection coll = mongo.getDB("test").getCollection("tweets");
		
		// test connection
//		DBObject myDoc = coll.findOne();
//		System.out.println(myDoc.get("text"));
		
		// fetch the relevant tweets
		DBCursor cursor = coll.find().skip(20).limit(20);
		
		ClausIE clausIE = new ClausIE();
        clausIE.initParser();
        
		try {
			while (cursor.hasNext()) {
				String tweet_text = cursor.next().get("text").toString();
				System.out.println();
				System.out.println(tweet_text);
				
				// process tweet texts with ClausIE
				clausIE.parse(tweet_text);
				clausIE.detectClauses();
		        clausIE.generatePropositions();
		        
		        // generate propositions
		        System.out.println("Propositions     : ");
		        String sep = "";
		        for (Proposition prop : clausIE.getPropositions()) {
		            System.out.println(sep + prop.toString());
		            sep = "                   ";
		        }
			}
		} finally {
			cursor.close();
		}
	}
}

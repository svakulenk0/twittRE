import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Calendar;


import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.mongodb.Mongo;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import de.mpii.clausie.ClausIE;
import de.mpii.clausie.Options;
import de.mpii.clausie.Proposition;

public class MongoExample {
	
	static int limitn = 4;
	static int skipn = 0;
	
	// settings for RDBMS
	static SQLiteJDBC sqllite = new SQLiteJDBC();
	static Connection c;
	static String idpipe = "clausie1test";
	static String dateNow =  new String();
	// placeholder, conf is missing so far
	static String confidence = "0.0";
	
	public static void store_relation (Proposition prop, Tweet tweet) {
		  try {
			      // sql INSERT query
				PreparedStatement ps = c.prepareStatement("INSERT INTO tweetREs (IDpipe, Date, IDtweet, CleanedText, s, p, o, confidence)" +
			            "VALUES (?,?,?,?,?,?,?,?);"); // " + dateNow + " 
			    
			    ps.setString(1, idpipe);
		        ps.setString(2, dateNow);
		        ps.setString(3, tweet.ID);
		        ps.setString(4, tweet.cleanedText.toString());
		        ps.setString(5, prop.subject());
		        ps.setString(6, prop.relation());
		        
		        try{
		        	ps.setString(7, prop.argument(0));
		        } catch (java.lang.IndexOutOfBoundsException ex) {
		        	ps.setString(7, "");
		        }
		        
		        ps.setString(8, confidence);
		        
			    ps.executeUpdate();
			    ps.close();
			  } catch ( Exception e ) {
			      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			      System.exit(0);
			    }
	}
	
	// process tweet texts with ClausIE
	public static void clausie_process (DBCursor cursor) {
		
		// initiate clausie
		ClausIE clausIE = new ClausIE();
        clausIE.initParser();
        Options my_options = clausIE.getOptions(); //.print(System.out, "# ");
        my_options.nary = false;
        my_options.lemmatize = true;
        my_options.processCcNonVerbs = true;
        
        try {
			while (cursor.hasNext()) {
				Tweet tweet = new Tweet(cursor.next());
				
				// test: print out tweets
//				System.out.println(tweet_text_clean);
				
//				System.out.print(tweet_text_clean + "\t");
				
				try {
					for (String sentence : tweet.cleanedText) {
						clausIE.parse(sentence);
						clausIE.detectClauses();
				        clausIE.generatePropositions();
				        
				        // generate propositions
	//			        String sep = ";";
				        for (Proposition prop : clausIE.getPropositions()) {
				        	// store relations
				        	store_relation (prop, tweet);
	//			            System.out.print(prop.argument(0));
				        }
					}
				} catch (java.lang.UnsupportedOperationException ex){
					
				}
				
//				System.out.println();
				
			}
		} finally {
			cursor.close();
			sqllite.close(c);
		}
	}
	
	public static void main(String[] args) throws IOException {
		
		String profileDirectory = "profiles";		
		try {
			DetectorFactory.loadProfile(profileDirectory);
		} catch (LangDetectException ex) {
	        System.out.println(ex);
	    }
		
		// connect to MongoDB tweet collection
		Mongo mongo = new Mongo("localhost", 27017);
		DBCollection coll = mongo.getDB("test").getCollection("tweets");
		
		// test connection
//		DBObject myDoc = coll.findOne();
//		System.out.println(myDoc.get("text"));
		
		// fetch the relevant tweets
		DBCursor cursor = coll.find().skip(skipn).limit(limitn);
		
		// connect to sqlite
		c = sqllite.init();
		
		// generate current date
	    Calendar currentDate = Calendar.getInstance();
	    SimpleDateFormat formatter= new SimpleDateFormat("dd/MM/yyyy");
	    dateNow = formatter.format(currentDate.getTime());
		
		clausie_process(cursor);
		
	}
}

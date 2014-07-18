import java.awt.List;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;


import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.mongodb.BasicDBObject;
import com.mongodb.Mongo;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import de.mpii.clausie.ClausIE;
import de.mpii.clausie.Options;
import de.mpii.clausie.Proposition;

public class MongoExample {
	
	static int limitn = 0;
	static int skipn = 0;
	static String dbname = "tweets"; // tweets - server-side test
	static String mongo_collection = "facup"; // "tweets", facup, snow
	
	// settings for RDBMS
	static SQLiteJDBC sqllite = new SQLiteJDBC();
	static Connection c;
	static String idsource = mongo_collection;
	static String idmethod = "clausie";
	static String dateNow =  new String();
	// placeholder, conf is missing so far
	static String confidence = "0.0";
	static ArrayList <Long> apt = new ArrayList <Long> ();
	static ArrayList <Long> act = new ArrayList <Long> ();
	
	public static double average(ArrayList<Long> list) {
	    // 'average' is undefined if there are no elements in the list.
	    if (list == null || list.isEmpty())
	        return 0.0;
	    // Calculate the summation of the elements in the list
	    long sum = 0;
	    int n = list.size();
	    // Iterating manually is faster than using an enhanced for loop.
	    for (int i = 0; i < n; i++)
	        sum += list.get(i);
	    // We don't want to perform an integer division, so the cast is mandatory.
	    return ((double) sum) / n;
	}
	
	public static void store_relation (Proposition prop, Tweet tweet) {
		  try {
			      // sql INSERT query
				PreparedStatement ps = c.prepareStatement("INSERT INTO tweetREs (Source, Method, Date, IDtweet, CleanedText, s, p, o, confidence)" +
			            "VALUES (?,?,?,?,?,?,?,?,?);"); // " + dateNow + " 
			    
			    ps.setString(1, idsource);
			    ps.setString(2, idmethod);
		        ps.setString(3, dateNow);
		        ps.setString(4, tweet.ID);
		        ps.setString(5, tweet.cleanedText.toString());
		        ps.setString(6, prop.subject());
		        ps.setString(7, prop.relation());
		        
		        try{
		        	ps.setString(8, prop.argument(0));
		        } catch (java.lang.IndexOutOfBoundsException ex) {
		        	ps.setString(8, "");
		        }
		        
		        ps.setString(9, confidence);
		        
			    ps.executeUpdate();
			    ps.close();
			    c.commit();
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
						
						// Average parse time
				        long start = System.currentTimeMillis();
						clausIE.parse(sentence);
						long end = System.currentTimeMillis();
				        apt.add((long) ((end - start) / 1000.));
				        
				        // clause detection
				        // Average ClausIE time
				        start = System.currentTimeMillis();
						clausIE.detectClauses();
				        clausIE.generatePropositions();
				        end = System.currentTimeMillis();
				        act.add((long) ((end - start) / 1000.));
				        
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
			System.out.println("Average parse time: " + average(apt));
			System.out.println("Average ClausIE time: " + average(act));
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
		Mongo mongo = new Mongo("rs210522.rs.hosteurope.de", 27017); //localhost
		DBCollection coll = mongo.getDB(dbname).getCollection(mongo_collection);
		
		// test connection
//		DBObject myDoc = coll.findOne();
//		System.out.println(myDoc.get("text"));
		
		// fetch the relevant tweets
		BasicDBObject query = new BasicDBObject("text", new BasicDBObject( "$exists", true ));
		DBCursor cursor = coll.find(query).skip(skipn).limit(limitn);
		
		// connect to sqlite
		String table_sql = "DROP TABLE tweetREs; CREATE TABLE tweetREs (IDrel integer primary key autoincrement, Source TEXT, Method TEXT, Date TEXT, IDtweet TEXT,CleanedText TEXT, s TEXT, p TEXT, o TEXT, confidence REAL);";
		c = sqllite.init(table_sql);
		
		// generate current date
	    Calendar currentDate = Calendar.getInstance();
	    SimpleDateFormat formatter= new SimpleDateFormat("dd/MM/yyyy");
	    dateNow = formatter.format(currentDate.getTime());
		
		clausie_process(cursor);
		
	}
}

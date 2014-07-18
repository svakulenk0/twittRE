import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.springframework.util.StringUtils;

import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.trees.EnglishGrammaticalRelations;
import edu.stanford.nlp.trees.GrammaticalRelation;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.WordStemmer;
import edu.stanford.nlp.trees.semgraph.SemanticGraph;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.ling.*; 
import edu.stanford.nlp.ling.CoreAnnotations.*; 

import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.mongodb.BasicDBObject;
import com.mongodb.Mongo;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import de.mpii.clausie.ClausIE;
import de.mpii.clausie.Options;
import de.mpii.clausie.Proposition;

public class MongoExample_nAry {
	
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
	static ArrayList <Double> apt = new ArrayList <Double> ();
	static ArrayList <Double> act = new ArrayList <Double> ();
	
	public static String normalize(String text_string) {
		ArrayList<String> stopWords = new ArrayList<String>( Arrays.asList("a", "the") );

		//use a linked list to make removal faster, you don't need random access here
		ArrayList<String> text_list = new ArrayList<String>(Arrays.asList(text_string.toLowerCase().split(" ")));
		text_list.removeAll(stopWords);
		
		String normalized_string = StringUtils.collectionToDelimitedString(text_list, " ");
		
		
//		System.out.println(normalized_string + ",");
		return normalized_string;
	}
	
	public static BigDecimal average(ArrayList<Double> list) {
	    // 'average' is undefined if there are no elements in the list.
	    if (list == null || list.isEmpty())
	        return BigDecimal.ZERO;
	    // Calculate the summation of the elements in the list
	    double sum = 0;
	    int n = list.size();
	    // Iterating manually is faster than using an enhanced for loop.
	    for (int i = 0; i < n; i++)
	        sum += list.get(i);
	    // We don't want to perform an integer division, so the cast is mandatory.
	    BigDecimal avg = new BigDecimal( ((double) sum) / n );
	    return avg.round(new MathContext(3, RoundingMode.HALF_UP));
	}
	
	public static void store_relation (Proposition prop, Tweet tweet) {
		  try {
			      // sql INSERT query
				PreparedStatement ps = c.prepareStatement("INSERT INTO tweetREs (Source, Method, Date, IDtweet, CleanedText, s, s_Type, p, o, o_Type, confidence)" +
			            "VALUES (?,?,?,?,?,?,?,?,?,?,?);"); // " + dateNow + " 
			    
			    ps.setString(1, idsource);
			    ps.setString(2, idmethod);
		        ps.setString(3, dateNow);
		        ps.setString(4, tweet.ID);
		        ps.setString(5, tweet.cleanedText.toString());
		        String s = prop.subject();
		        ps.setString(6, normalize(s));
		        
		        // Stanford NER's type
		        ArrayList<String> s_NERs = new ArrayList<String> ();
//		        System.out.println(s);
		        
		        // if any ne is in subject assign the corresponding key tag
	        	for (Entry<String, List<String>> e : tweet.NERs.entrySet()) {
	        		for (String value : e.getValue()) {
        				if (s.equals(value)) { //s.contains(value)
    	        			s_NERs.add(e.getKey());
//    	        			System.out.println(s_ner);
    	        		}
	        		}
	        	}
	        	String s_ner = StringUtils.collectionToCommaDelimitedString(s_NERs);
//	        	System.out.println(s_ner);
	        	
		        ps.setString(7, s_ner); 
		        ps.setString(8, normalize(prop.relation()));
		        
		        int index = 0;
		        while ( index < prop.noArguments() ) {
		        	String o = prop.argument(index);
		        	ps.setString(9, normalize(o));
		        	
		        	// Stanford NER's type
		        	ArrayList<String> o_NERs = new ArrayList<String> ();
//			        System.out.println(s);
			        
			        // if any ne is in subject assign the corresponding key tag
		        	for (Entry<String, List<String>> e : tweet.NERs.entrySet()) {
		        		for (String value : e.getValue()) {
//	        				System.out.println(value);
		        			if (o.equals(value)) {
	    	        			o_NERs.add(e.getKey());
//	    	        			System.out.println(s_ner);
	    	        		}
		        		}
		        	}
		        	String o_ner = StringUtils.collectionToCommaDelimitedString(o_NERs);
		        	System.out.println(o_ner);
		        	
		        	ps.setString(10, o_ner);

			        ps.setString(11, confidence);
				    ps.executeUpdate();
				    index++;
		        }
		        
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
        Options my_options = clausIE.getOptions();
        my_options.lemmatize = true; // lemmatize only verbs modified by SV
        my_options.print(System.out, "# ");
        
        try {
			while (cursor.hasNext()) {
				Tweet tweet = new Tweet(cursor.next());
				
				// test: print out tweets
//				System.out.println(tweet_text_clean);
				
//				System.out.print(tweet_text_clean + "\t");
				
				try {
					for (String sentence : tweet.cleanedText) {
						
						// Average parse time
				        double start = System.currentTimeMillis();
						clausIE.parse(sentence);
						double end = System.currentTimeMillis();
				        apt.add((double) ((end - start) / 1000.));
				        
				        // clause detection
				        // Average ClausIE time
				        start = System.currentTimeMillis();
						clausIE.detectClauses();
				        clausIE.generatePropositions();
				        end = System.currentTimeMillis();
				        act.add((double) ((end - start) / 1000.));
				        
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
		BasicDBObject query = new BasicDBObject("cleaned_sentences", new BasicDBObject( "$exists", true ));
		DBCursor cursor = coll.find(query).skip(skipn).limit(limitn);
		
		// connect to sqlite
		String table_sql = "DROP TABLE tweetREs; CREATE TABLE tweetREs (IDrel integer primary key autoincrement, Source TEXT, Method TEXT, Date TEXT, IDtweet TEXT,CleanedText TEXT, s TEXT, s_Type TEXT, p TEXT, o TEXT, o_Type TEXT, confidence REAL);";
		c = sqllite.init(table_sql);
		
		// generate current date
	    Calendar currentDate = Calendar.getInstance();
	    SimpleDateFormat formatter= new SimpleDateFormat("dd/MM/yyyy");
	    dateNow = formatter.format(currentDate.getTime());
		
		clausie_process(cursor);
		
	}
}

package prepro;


import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.util.StringUtils;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import db.SQLiteJDBC;
import de.mpii.clausie.ClausIE;
import de.mpii.clausie.Options;
import de.mpii.clausie.Proposition;

public class RExtractor {
	
    private DBCollection coll;
    private Mongo mongoClient;
    private String dbname = "tweets";
    private String profileDirectory = "profiles";
    private List<DBCollection> collections = new ArrayList<DBCollection>();
    private NameThatEntity nte;
    
    private ArrayList <Double> apt = new ArrayList <Double> ();
    private ArrayList <Double> act = new ArrayList <Double> ();
	
    // settings for RDBMS
    private SQLiteJDBC sqllite = new SQLiteJDBC();
    private static Connection c;
    private static String dateNow =  new String();
    private static String idsource;
    private static String idmethod = "clausie";

    public void StartCon(String mongo_collection) {
    	
    	idsource = mongo_collection;
    	
        try {
        	nte = new NameThatEntity();
            DetectorFactory.loadProfile(profileDirectory);            
            mongoClient = new Mongo("rs210522.rs.hosteurope.de", 27017); //"rs210522.rs.hosteurope.de" localhost
            coll = mongoClient.getDB(dbname).getCollection(mongo_collection);

        } catch (IOException ex) {
            System.out.println("Could Not Connect to the Database. " + ex);
        } catch (LangDetectException ex) {
            System.out.println("Language profiles are not in the correct folder!. " + ex);
        }
        
        // connect to sqlite
 		String table_sql = "DROP TABLE tweetREs; CREATE TABLE tweetREs (IDrel integer primary key autoincrement, Source TEXT, Method TEXT, Date TEXT, IDtweet TEXT,CleanedText TEXT, s TEXT, s_Type TEXT, p TEXT, o TEXT, o_Type TEXT);";
 		table_sql = ""; // CONTINUE do not overwrite
 		c = sqllite.init(table_sql);
     		
 		// generate current date
 	    Calendar currentDate = Calendar.getInstance();
 	    SimpleDateFormat formatter= new SimpleDateFormat("dd/MM/yyyy");
 	    dateNow = formatter.format(currentDate.getTime());
    }

    public void Process(int limitn, int skipn) {

        BasicDBObject query = new BasicDBObject("clustering_pos", new HashMap<String, List<String>>()).append("language", "en"); // "text", new BasicDBObject("$exists", true) CONTINUE
        DBCursor cursor = coll.find(query).skip(skipn).limit(limitn);

        // initiate clausie
 		ClausIE clausIE = new ClausIE();
 		clausIE.initParser();
 		Options my_options = clausIE.getOptions();
 		my_options.lemmatize = true; // lemmatize only verbs modified by SV
// 		my_options.print(System.out, "# ");
 		System.out.println("Running...");
 		
        while (cursor.hasNext()) {
            DBObject old_temp = cursor.next();//Need to keep this one so we can update later.
            Tweet tweet = new Tweet(old_temp, nte);
            // process each sentence with ClausIE
            extractRelations(clausIE, tweet);
            tweet.updateObject(coll);
            // remove object to free memory
            tweet = null;
        }
        cursor.close();
        mongoClient.close();
        sqllite.close(c);
		System.out.println("Average parse time: " + average(apt));
		System.out.println("Average ClausIE time: " + average(act));
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
    
    public static String normalize(String text_string) {
		ArrayList<String> stopWords = new ArrayList<String>( Arrays.asList("a", "the") );

		//use a linked list to make removal faster, you don't need random access here
		ArrayList<String> text_list = new ArrayList<String>(Arrays.asList(text_string.toLowerCase().split(" ")));
		text_list.removeAll(stopWords);
		
		String normalized_string = StringUtils.collectionToDelimitedString(text_list, " ");
		
		
//		System.out.println(normalized_string + ",");
		return normalized_string;
	}
    
    public void extractRelations(ClausIE clausIE, Tweet tweet) {
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
    }
    
    public static void store_relation (Proposition prop, Tweet tweet) {
		  try {
			      // sql INSERT query
				PreparedStatement ps = c.prepareStatement("INSERT INTO tweetREs (Source, Method, Date, IDtweet, CleanedText, s, s_Type, p, o, o_Type)" +
			            "VALUES (?,?,?,?,?,?,?,?,?,?);"); // " + dateNow + " 
			    
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
//  	        			System.out.println(s_ner);
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
//		        	System.out.println(o_ner);
		        	
		        	ps.setString(10, o_ner);

//			        ps.setString(11, confidence);
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
}

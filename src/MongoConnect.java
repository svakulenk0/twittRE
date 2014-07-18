
import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class MongoConnect {

    private int limitn = 3;
    private int skipn = 0;
    
    // settings for RDBMS
 	static SQLiteJDBC sqllite = new SQLiteJDBC();
 	static Connection c;

    private DBCollection coll;
    private Set<String> collNames;   
    private String dbname = "tweets";
    private String profileDirectory = "profiles";
    private String mongo_collection = "facup"; // "tweets", facup, snow
    private List<DBCollection> collections = new ArrayList<DBCollection>();

    private String idmethod = "clausie";
    private String dateNow = "";
    
//    public void MongoConnect(){
//        StartCon();
////        langDetect();
//    }

    public void StartCon() {
        try {
            DetectorFactory.loadProfile(profileDirectory);
            Mongo mongoClient = new Mongo("rs210522.rs.hosteurope.de", 27017);
            DB db = mongoClient.getDB(dbname);

            coll = db.getCollection(mongo_collection); //Need to disable for multiple collections

////====== Enable for multiple collections ====== 
//            collNames = db.getCollectionNames();
//            DBCollection tmp;
//            for(String coll : collNames){
//                tmp = db.getCollection(coll);
//                collections.add(tmp);
//            }        
////=============================================            
        } catch (IOException ex) {
            System.out.println("Could Not Connect to the Database. " + ex);
        } catch (LangDetectException ex) {
            System.out.println("Language profiles are not in the correct folder!. " + ex);
        }
    }
    
    public void aggregate() {
    	// connect to sqlite
    	String table_sql = "DROP TABLE NERs; CREATE TABLE NERs (IDent integer primary key autoincrement, Key TEXT, Entity TEXT, Count integer);"; 
		c = sqllite.init(table_sql);
		
    	ArrayList<String> keys = new ArrayList<String>( Arrays.asList("LOCATION", "ORGANIZATION", "PERSON") );
    	for (String key : keys) {
	    	DBObject unwind = new BasicDBObject("$unwind", "$clustering_ner." + key);
	    	
	    	//the $group operation
	    	DBObject groupFields = new BasicDBObject( "_id", "$clustering_ner." + key);
	    	groupFields.put("total", new BasicDBObject( "$sum", 1));
	    	DBObject group = new BasicDBObject("$group", groupFields);
	    	
	    	AggregationOutput output = coll.aggregate(unwind, group);
	    	
	    	for (DBObject result : output.results()) {
	    	    // translate from json to vars
	    		JSONObject obj=(JSONObject) JSONValue.parse(result.toString());
	    		// e.g. LOCATION London 120
	    		 System.out.println(key + obj.get("_id").toString() + obj.get("total").toString());
	    	   
	    		// save to sqlite
	    		try {
	    			// sql INSERT query
					PreparedStatement ps = c.prepareStatement("INSERT INTO NERs (Key, Entity, Count) VALUES (?,?,?)");
					ps.setString(1, key);
					ps.setString(2, obj.get("_id").toString().toUpperCase());
				    ps.setString(3, obj.get("total").toString());
				    ps.executeUpdate();
				    ps.close();
				    c.commit();
				} catch ( Exception e ) {
				      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
				      System.exit(0);
				}
	    	}
    	}
    }

    public void langDetect() {
        String text = "";

        BasicDBObject query = new BasicDBObject("text", new BasicDBObject("$exists", true));
        DBCursor cursor = coll.find().skip(skipn).limit(limitn);

        while (cursor.hasNext()) {
            text = cursor.next().get("text").toString();
            System.out.println(text);
//            try {
//                Detector detector = DetectorFactory.create();
//                detector.append(text);
//                String lang = detector.detect();
//                
//                
//                
//            } catch (LangDetectException ex) {
//                System.out.println("Could not detect language! " + ex);
//            }
        }
    }

}

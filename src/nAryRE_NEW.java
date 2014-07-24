import prepro.MongoLanguageCleaner;
import prepro.RExtractor;

public class nAryRE_NEW {

    public static void main(String[] args) {
    	int limitn = 0;
    	int skipn = 210021;
    	String mongo_collection = "snow"; // tweets, facup, snow
    	
        RExtractor rex = new RExtractor();
        rex.StartCon(mongo_collection);
        rex.Process(limitn, skipn);
    }
    
}

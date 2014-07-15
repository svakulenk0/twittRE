
public class MongoTestMain {

    public static void main(String[] args) {     
        MongoLanguageCleaner mgc = new MongoLanguageCleaner();
        mgc.StartCon();
        mgc.preProcess();
    }
    
}

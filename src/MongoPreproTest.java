import prepro.MongoLanguageCleaner;


public class MongoPreproTest {

    public static void main(String[] args) {     
        MongoLanguageCleaner mgc = new MongoLanguageCleaner();
        mgc.StartCon();
        mgc.preProcess();
    }
    
}

// class to aggregate json from mongo to an sqllite table
public class MongoAggrSQL {
	
	public static void main(String[] args) {
		// connect to mongo
		MongoConnect connect_mongo = new MongoConnect();
		connect_mongo.StartCon();
		// aggregate from mongo
		connect_mongo.aggregate();
	}
}

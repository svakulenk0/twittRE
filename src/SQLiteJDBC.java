import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class SQLiteJDBC {
	
  private static String dateNow;
  
  public Connection init()
  {
    Connection c = null;
    Statement stmt = null;
    try {
      Class.forName("org.sqlite.JDBC");
      c = DriverManager.getConnection("jdbc:sqlite:data/test.db");
      c.setAutoCommit(false);
      System.out.println("Opened database successfully");

      stmt = c.createStatement();
      
      // update tweetREs table
      String sql = "DROP TABLE tweetREs;"; 
      stmt.executeUpdate(sql);
      sql = "CREATE TABLE tweetREs (IDrel integer primary key autoincrement, IDpipe TEXT, Date TEXT, IDtweet TEXT,CleanedText TEXT, s TEXT, p TEXT, o TEXT, confidence REAL);"; 
      stmt.executeUpdate(sql);
      
      // generate current date
      Calendar currentDate = Calendar.getInstance();
      SimpleDateFormat formatter= new SimpleDateFormat("dd/MM/yyyy");
      dateNow = formatter.format(currentDate.getTime());
      
      stmt.close();

    } catch ( Exception e ) {
      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
      System.exit(0);
    }
	return c;
  }
  
  public static void insert(Connection c)  {
	
	 }
  
  public static void close(Connection c)  {
	  try {
	    c.commit();
	    c.close();
	  } catch ( Exception e ) {
	      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	      System.exit(0);
	    }
	    System.out.println("Records created successfully");
	  }
  
}
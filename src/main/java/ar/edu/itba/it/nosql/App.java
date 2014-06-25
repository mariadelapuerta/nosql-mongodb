package ar.edu.itba.it.nosql;

import java.net.UnknownHostException;
import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class App {

	static String[] status = {"pending", "pending", "delivered", "canceled"};
	static String[] returnflag = {"true", "true", "false", "false"};
	static String[] qty = {"1", "2", "3", "4"};
	static String[] price = {"10", "20", "30", "40"};
	static String[] disc = {"10", "20", "30", "40"};
	static String[] tax = {"1", "2", "3", "4"};
	static String[] ship = {"pending", "pending", "delivered", "canceled"};
	
	public static void main(String[] args) {
		try {
			MongoClient mongo = new MongoClient("localhost", 27017);
			DB db = mongo.getDB("db");
			DBCollection table = db.getCollection("user");
			
			BasicDBObject document;
			for(int i=0; i < status.length; i++) {
				document = new BasicDBObject();
				document.append("linestatus", status[i]);
				document.append("returnflag", returnflag[i]);
				document.append("quantity", qty[i]);
				document.append("extendedprice", price[i]);
				document.append("discount", disc[i]);
				document.append("tax", tax[i]);
				document.append("shipdate", ship[i]);
				table.insert(document);
			}
			
			System.out.println("All items: " + table.getCount());
			
			DBCursor cursor = table.find();
			try {
			       while(cursor.hasNext()) {
			          //System.out.println(cursor.next());
			    	   cursor.next();
			       }
			    } finally {
			       cursor.close();
			    }
			
			BasicDBObject query = new BasicDBObject();
			query.put("shipdate", "pending");
			DBCursor c = table.find(query); 
			try {
			       while(c.hasNext()) {
			           System.out.println(c.next());
			       }
			    } finally {
			       cursor.close();
			    }
			
			table.drop();
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}

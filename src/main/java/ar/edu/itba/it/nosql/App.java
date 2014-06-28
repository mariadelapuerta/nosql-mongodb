package ar.edu.itba.it.nosql;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

public class App {

	static String[] status = { "pending", "pending", "delivered", "canceled" };
	static String[] returnflag = { "true", "true", "true", "false" };
	static Integer[] qty = { 1, 2, 3, 4 };
	static Integer[] price = { 10, 20, 30, 40 };
	static Double[] disc = { 0.1, 0.2, 0.3, 0.4 };
	static Double[] tax = { 0.1, 0.2, 0.3, 0.4 };
	static String[] ship = { "pending", "pending", "delivered", "canceled" };

	static String[] name = { "Pablo", "Maria", "Federico", "Agustin", "Andres" };
	static String[] address = { "a", "b", "c", "d", "e" };
	static String[] phone = { "1", "2", "3", "4", "5" };
	static Double[] actbal = { 1.0, 2.0, 3.0, 4.0, 5.0 };
	static String[] comment = { "a", "b", "c", "d", "e" };
	static String[] nationname = { "Arg", "Arg", "Bra", "Col", "Chi" };
	static String[] regionname = { "BA", "BA", "BAH", "BOG", "SAN" };

	public static void main(String[] args) throws IOException {
		try {
			MongoClient mongo = new MongoClient("localhost", 27017);
			DB db = mongo.getDB("db");
			DBCollection collection = db.getCollection("user");

			//loadFirstQueryData(collection);
			
			loadSecondQueryData(collection);

			System.out.println("All items: " + collection.getCount());

			DBCursor cursor = collection.find();
			try {
				while (cursor.hasNext()) {
					System.out.println(cursor.next());
				}
			} finally {
				cursor.close();
			}

			//firstQuery(collection);

			collection.drop();

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void loadFirstQueryData(DBCollection collection) {
		BasicDBObject document;
		for (int i = 0; i < status.length; i++) {
			document = new BasicDBObject();
			Date date = new Date(System.currentTimeMillis());
			document.append("linestatus", status[i]);
			document.append("returnflag", returnflag[i]);
			document.append("quantity", qty[i]);
			document.append("extendedprice", price[i]);
			document.append("discount", disc[i]);
			document.append("tax", tax[i]);
			document.append("shipdate", date.toString());
			collection.insert(document);
		}
	}

	public static void firstQuery(DBCollection collection) {
		DBObject match = new BasicDBObject("$match", new BasicDBObject(
				"returnflag", "true"));
		DBObject fields = new BasicDBObject("linestatus", 1);
		fields.put("returnflag", 1);
		fields.put("quantity", 1);
		fields.put("tax", 1);
		fields.put("extendedprice", 1);
		fields.put("discount", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);

		Map<String, Object> map = new HashMap<String, Object>();
		map.put("returnflag", "$returnflag");
		map.put("linestatus", "$linestatus");
		DBObject groupFields = new BasicDBObject("_id", new BasicDBObject(map));
		// Suma de cantidades
		groupFields.put("sum_qty", new BasicDBObject("$sum", "$quantity"));

		// Suma de extended prices
		groupFields.put("sum_base_price", new BasicDBObject("$sum",
				"$extendedprice"));

		// Suma de los precios con descuento
		BasicDBList sum_disc_price = new BasicDBList();
		BasicDBList discount = new BasicDBList();
		sum_disc_price.add("$extendedprice");
		discount.add(1);
		discount.add("$discount");
		sum_disc_price.add(new BasicDBObject("$subtract", discount));
		groupFields.put("sum_disc_price", new BasicDBObject("$sum",
				new BasicDBObject("$multiply", sum_disc_price)));

		// Suma de precio final con impuesto
		BasicDBList tax = new BasicDBList();
		BasicDBList sum_charge = new BasicDBList();
		tax.add(1);
		tax.add("$tax");
		sum_charge.add(new BasicDBObject("$add", tax));
		sum_charge.add(new BasicDBObject("$subtract", discount));
		sum_charge.add("$extendedprice");
		groupFields.put("sum_charge", new BasicDBObject("$sum",
				new BasicDBObject("$multiply", sum_charge)));

		// Promedio de cantidad
		groupFields.put("avg_qty", new BasicDBObject("$avg", "$quantity"));

		// Promedio de precio
		groupFields.put("avg_price",
				new BasicDBObject("$avg", "$extendedprice"));

		// Promedio de descuento
		groupFields.put("avg_disc", new BasicDBObject("$avg", "$discount"));

		// Count Order
		groupFields.put("count_order", new BasicDBObject("$sum", 1));

		DBObject group = new BasicDBObject("$group", groupFields);

		// Finally the $sort operation
		DBObject sort = new BasicDBObject("$sort", new BasicDBObject(
				"count_order", -1));
		DBObject sort2 = new BasicDBObject("$sort", new BasicDBObject(
				"sum_base_price", -1));

		List<DBObject> pipeline = Arrays.asList(match, project, group, sort,
				sort2);
		AggregationOutput c = collection.aggregate(pipeline);

		for (DBObject a : c.results()) {
			System.out.println(a);
		}

	}

	public static void loadSecondQueryData(DBCollection collection)
			throws IOException {
		// BasicDBObject document;
		// for (int i = 0; i < name.length; i++) {
		// document = new BasicDBObject();
		// document.append("name", name[i]);
		// document.append("address", address[i]);
		// document.append("phone", phone[i]);
		// document.append("actbal", actbal[i]);
		// document.append("comment", comment[i]);
		// document.append("nationname", nationname[i]);
		// document.append("regionname", regionname[i]);
		// }

		Path path = FileSystems.getDefault().getPath("docs", "supplier.json");

		String supp = new String(Files.readAllBytes(path));

		Object o = JSON.parse(supp);
		DBObject document = (DBObject) o;
		
		collection.insert(document);
	}

}

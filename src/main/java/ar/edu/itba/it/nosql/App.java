package ar.edu.itba.it.nosql;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.NewBSONDecoder;

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

	public static void main(String[] args) throws IOException {
		try {
			MongoClient mongo = new MongoClient("localhost", 27017);
			DB db = mongo.getDB("db");
			DBCollection collection = db.getCollection("user");

			// loadFirstQueryData(collection);

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

			// firstQuery(collection);
			secondQuery(collection, "Buenos Aires", 3000, "Corcho");
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

		int i = 1;
		for (i = 1; i < 4; i++) {

			Path path = FileSystems.getDefault().getPath("docs",
					"supplier" + i + ".json");

			String supp = new String(Files.readAllBytes(path));

			Object o = JSON.parse(supp);
			DBObject document = (DBObject) o;

			collection.insert(document);
		}
	}

	public static void secondQuery(DBCollection collection, String regionName,
			int size, String type) {

		// Match por Region
		DBObject match = new BasicDBObject("$match", new BasicDBObject(
				"regionname", regionName));

		DBObject fields = new BasicDBObject("acctbal", 1);
		fields.put("name", 1);
		fields.put("regionname", 1);
		fields.put("partkey", 1);
		fields.put("mfgr", 1);
		fields.put("address", 1);
		fields.put("phone", 1);
		fields.put("comment", 1);
		fields.put("parts", 1);
		fields.put("_id", 0);
		DBObject project = new BasicDBObject("$project", fields);

		DBObject unwinds = new BasicDBObject("$unwind", "$parts");

		// ----------------CRITERIO PARA LAS PARTES------------------------

		// Match por Size
		DBObject sizeCriteria = new BasicDBObject("$match", new BasicDBObject(
				"parts.size", new BasicDBObject("$eq", size)));

		// Match por Type
		DBObject typeCriteria = new BasicDBObject("$match", new BasicDBObject(
				"parts.type", type));

		// TODO: ESTO NO ESTA FUNCIONANDO ASI QUE LO HAGO POR SEPARADO.
		// Creo la criteria del AND de los matches
		// BasicDBList and = new BasicDBList();
		// and.add(sizeCriteria);
		// and.add(typeCriteria);
		//
		// DBObject partCriteria = new BasicDBObject("$and", and);
		// --------------------------------------------------------

		DBObject groupFields = new BasicDBObject("_id", "$_id");
		DBObject group = new BasicDBObject("$group", groupFields);

		List<DBObject> pipeline = Arrays.asList(match, project, unwinds,
				sizeCriteria, typeCriteria);
		AggregationOutput c = collection.aggregate(pipeline);

		for (DBObject a : c.results()) {
			System.out.println(a);
		}

	}
}

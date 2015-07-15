package com.tune.flatbufferreader;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Time;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


/**
 * Takes dates from json file, and search for matches.
 * @author fuat, devang
 *
 */
public class Initializer {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	static JSONObject jsonArray;

	/**
	 * Main program
	 * 
	 * @param args the file names.
	 * @throws SQLException sql exception.
	 * @throws IOException io exception.
	 * @throws ClassNotFoundException class not found.
	 * @throws JSONException json exception.
	 * @throws InterruptedException 
	 */

	public static void main(String[] args) throws SQLException, IOException,
			ClassNotFoundException, JSONException, InterruptedException {
/*
		if (args.length == 0) {
			System.out.println("To run this jar: java -jar <jarname> <JSON local file path>  <~/hadoop/etc/hadoop/core-site.xml>");
			System.exit(0);
		}
*/
		System.out.println("Starting the parser");
		List<String> files = new ArrayList<String>();
		List<String> folders = new ArrayList<String>();
		List<String> patterns = new ArrayList<String>();
		List<Integer> advertiser_id = new ArrayList<Integer>();
		List<Integer> prison_id = new ArrayList<Integer>();
		List<Integer> producer_id = new ArrayList<Integer>();

		JSONParser parser = new JSONParser();
		try {

			//Object obj = parser.parse(new FileReader("/home/devang/Desktop/IMP/property.json"));
			Object obj = parser.parse(new FileReader("/home/hadoop/devang/property.json"));
			jsonArray = (JSONObject) obj;

		} catch (ParseException e) {

			System.err.println("Cannot parse to json object! " + e);
		}

		JSONArray ad_Id = (JSONArray) jsonArray.get("Advertiser_ID");
		//gets advertiser ids based on given json file.
		for (int i = 0; i < ad_Id.size(); i++) {
			advertiser_id.add(Integer.parseInt((String) ad_Id.get(i)));
		}
		//takes dates as format of max date and min date.
		String[] min = ((String) jsonArray.get("Min")).split(" ");
		String[] max = ((String) jsonArray.get("Max")).split(" ");
		String[] mindate = min[0].split("-");
		String[] maxdate = max[0].split("-");
		String[] mintime = min[1].split(":");
		String[] maxtime = max[1].split(":");
		System.out.println("min" + min[0]);
		Date startdate = new Date(Integer.parseInt(mindate[0]),
				Integer.parseInt(mindate[1]) - 1, Integer.parseInt(mindate[2]));
		Date enddate = new Date(Integer.parseInt(maxdate[0]),
				Integer.parseInt(maxdate[1]) - 1, Integer.parseInt(maxdate[2]));
		Time starttime = new Time(Integer.parseInt(mintime[0]),
				Integer.parseInt(mintime[1]), Integer.parseInt(mintime[2]));
		Time endtime = new Time(Integer.parseInt(maxtime[0]),
				Integer.parseInt(maxtime[1]), Integer.parseInt(maxtime[2]));
		
		/*
		LocalTime starttime = LocalTime.of(Integer.parseInt(mintime[0]),
				Integer.parseInt(mintime[1]), Integer.parseInt(mintime[2]));
		LocalTime endtime = LocalTime.of(Integer.parseInt(maxtime[0]),
				Integer.parseInt(maxtime[1]), Integer.parseInt(maxtime[2]));
		LocalDate startdate = LocalDate.of(Integer.parseInt(mindate[0]),
				Integer.parseInt(mindate[1]), Integer.parseInt(mindate[2]));
		LocalDate enddate = LocalDate.of(Integer.parseInt(maxdate[0]),
				Integer.parseInt(maxdate[1]), Integer.parseInt(maxdate[2]));*/
		
		
		//reads prison id from json file
		JSONArray p_id = (JSONArray) jsonArray.get("Prison_ID");
		for (int i = 0; i < p_id.size(); i++) {
			prison_id.add(Integer.parseInt((String) p_id.get(i)));
		}

		//reads producer id from given json file.
		JSONArray pro_id = (JSONArray) jsonArray.get("Producer_ID");
		for (int i = 0; i < pro_id.size(); i++) {
			producer_id.add(Integer.parseInt((String) pro_id.get(i)));
		}
		
		//gets revision id.
		String revision_id = (String) jsonArray.get("Revision_ID");
		//gets sequence id.
		String sequence_num = (String) jsonArray.get("Sequence_num");
		
		Configuration conf = new Configuration();
		conf.addResource(new Path("/home/devang/Documents/hadoop/etc/hadoop/core-site.xml"));
		FileSystem fs1 = FileSystem.get(conf);
		
		folders = Utility.getfoldernames(startdate, enddate);

		patterns = Utility.getpatterns(revision_id,
				producer_id, sequence_num, advertiser_id, prison_id);

		files = Utility.getfiles(folders, patterns, fs1);

		Utility.getdata(files, starttime, endtime, conf, driverName);
	}

}
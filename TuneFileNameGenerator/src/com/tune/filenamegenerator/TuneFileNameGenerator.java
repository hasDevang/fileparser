package com.tune.filenamegenerator;


import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.mortbay.io.BufferDateCache;

import com.twitter.chill.Base64.InputStream;
/**
 * 
 * @author devang
 *
 */
public class TuneFileNameGenerator {
	
	/*This method is invoked from the getdata method and having two parameters. One is the drivername for the JDBC hive connection and another one is the configuration object.*/
	

	/*This method goes through each file in the files list and parse/filters the data that are requested.
	 * It writes the extracted data to the HDFS file and then call the loadtohive function to load that data to the hive table.*/
	
		
		public static List<String> getFilesfrom (String JSONpath, String hdfsPath)

		{
		JSONObject jsonArray = null;
		List<String> files = new ArrayList<String>();
		List<String> folders = new ArrayList<String>();
		List<String> patterns = new ArrayList<String>();
		List<Integer> advertiserId = new ArrayList<Integer>();
		List<Integer> prisonId = new ArrayList<Integer>();
		List<Integer> producerId = new ArrayList<Integer>();
		
		JSONParser parser = new JSONParser();
		try {

			//Object obj = parser.parse(new FileReader("/home/devang/Desktop/IMP/property.json"));
			Object obj = parser.parse(new FileReader(JSONpath));
			jsonArray = (JSONObject) obj;

		} catch (ParseException e) {

			System.err.println("Cannot parse to json object! " + e);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		JSONArray ad_Id = (JSONArray) jsonArray.get("Advertiser_ID");
		//gets advertiser ids based on given json file.
		for (int i = 0; i < ad_Id.size(); i++) {
			advertiserId.add(Integer.parseInt((String) ad_Id.get(i)));
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
		
		//reads prison id from json file
		JSONArray pId = (JSONArray) jsonArray.get("Prison_ID");
		for (int i = 0; i < pId.size(); i++) {
			prisonId.add(Integer.parseInt((String) pId.get(i)));
		}

		//reads producer id from given json file.
		JSONArray batcherId = (JSONArray) jsonArray.get("Producer_ID");
		for (int i = 0; i < batcherId.size(); i++) {
			producerId.add(Integer.parseInt((String) batcherId.get(i)));
		}
		
		//gets revision id.
		String revisionId = (String) jsonArray.get("Revision_ID");
		//gets sequence id.
		String sequenceNumber = (String) jsonArray.get("Sequence_num");
		
		Configuration conf = new Configuration();
		conf.addResource(new Path(hdfsPath));
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		folders = Utility.getFolderNames(startdate, enddate);

		patterns = Utility.getPatterns(revisionId,
				producerId, sequenceNumber, advertiserId, prisonId);

		files = Utility.getFiles(folders, patterns, fs);
		System.out.println(files.size());
			return files;
			
		}
		public static void main (String[] args)
		{
			List<String> files = getFilesfrom("/home/devang/Desktop/IMP/property.json","/home/devang/Documents/hadoop/etc/hadoop/core-site.xml");
			for(int j =0 ; j<files.size(); j++)
			{
			System.out.println("File :"+ files.get(j));
			}
		}
}
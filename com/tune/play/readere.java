package play;

import java.awt.List;
import java.beans.Statement;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jets3t.service.utils.TimeFormatter;
import org.json.JSONException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import scala.tools.cmd.Parser;
import akka.pattern.Patterns;
import MyGame.rawLog;

public class readere {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	static JSONObject jsonArray;
	public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException, JSONException
	{
		System.out.println("Starting the parser");
		ArrayList<String> files = new ArrayList<String>();
		ArrayList<String> folders = new ArrayList<String>();
		ArrayList<String> patterns = new ArrayList<String>();
		
		//Properties pro = Utility.readParameters("/home/devang/Desktop/IMP/property.properties");
		JSONParser parser = new JSONParser();
		try {
			//Object obj = parser.parse(new FileReader("/home/devang/Desktop/IMP/property.json"));
			Object obj = parser.parse(new FileReader("/home/hadoop/devang/property.json"));
			jsonArray = (JSONObject) obj;
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		ArrayList<Integer> advertiser_id = new ArrayList<Integer>();
		JSONArray ad_Id = (JSONArray) jsonArray.get("Advertiser_ID");
		for (int i =0 ; i<ad_Id.size() ; i++)
		{
			//int v= Integer.parseInt(advertisers[i]);
			//System.out.println(Integer.parseInt(advertisers[i]));
			advertiser_id.add(Integer.parseInt((String) ad_Id.get(i)));
		}
		String[] min =((String) jsonArray.get("Min")).split(" ");
		String[] max =((String) jsonArray.get("Max")).split(" ");
		String[] mindate = min[0].split("-");
		String[] maxdate = max[0].split("-");
		String[] mintime = min[1].split(":");
		String[] maxtime = max[1].split(":");
		System.out.println("min"+ min[0]);
		Date startdate = new Date(Integer.parseInt(mindate[0]), Integer.parseInt(mindate[1])-1,Integer.parseInt( mindate[2]));
		Date enddate= new Date(Integer.parseInt(maxdate[0]), Integer.parseInt(maxdate[1])-1,Integer.parseInt( maxdate[2]));
		Time starttime = new Time(Integer.parseInt(mintime[0]), Integer.parseInt(mintime[1]), Integer.parseInt(mintime[2]));
		Time endtime = new Time(Integer.parseInt(maxtime[0]), Integer.parseInt(maxtime[1]), Integer.parseInt(maxtime[2]));
		
		ArrayList<Integer> prison_id = new ArrayList<Integer>();
		JSONArray p_id = (JSONArray) jsonArray.get("Prison_ID");
		for (int i =0 ; i<p_id.size() ; i++)
		{
			//int v= Integer.parseInt(advertisers[i]);
			//System.out.println(Integer.parseInt(advertisers[i]));
			prison_id.add(Integer.parseInt((String) p_id.get(i)));
		}
		
		ArrayList<Integer> producer_id = new ArrayList<Integer>();
		JSONArray pro_id = (JSONArray) jsonArray.get("Producer_ID");
		for (int i =0 ; i<pro_id.size() ; i++)
		{
			//int v= Integer.parseInt(advertisers[i]);
			//System.out.println(Integer.parseInt(advertisers[i]));
			producer_id.add(Integer.parseInt((String) pro_id.get(i)));
		}
		
		String revision_id = (String) jsonArray.get("Revision_ID");
		
	
		String sequence_num= (String) jsonArray.get("Sequence_num");
		
		Configuration conf= new Configuration();
		conf.addResource(new Path("/home/devang/Documents/hadoop/etc/hadoop/core-site.xml"));
		FileSystem fs1 = FileSystem.get(conf);
		
		Utility.getfoldernames(folders, startdate, enddate);
		
		Utility.getpatterns(patterns, startdate, enddate, revision_id, producer_id, sequence_num,
				advertiser_id, prison_id);
		
		Utility.getfiles(files, folders, patterns, fs1);
		
		Utility.getdata(files, starttime, endtime, conf, driverName);
	}

}
		
		
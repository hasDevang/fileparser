package com.tune.flatbufferreader;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import org.apache.hadoop.hive.ql.exec.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.mortbay.io.BufferDateCache;

import com.tune.flatbufferreader.FileReader;
import com.tune.flatbufferreader.Rawlog;
import com.tune.flatbufferreader.Reader;
import com.twitter.chill.Base64.InputStream;
/**
 * 
 * @author devang
 *
 */
public class Utility {
	
	/*This method is invoked from the getdata method and having two parameters. One is the drivername for the JDBC hive connection and another one is the configuration object.*/
	
	public static void loaddatatohive(Configuration conf, String driverName) throws SQLException, InterruptedException, ClassNotFoundException
	{
		Connection con=null;
		Statement st = null;
		
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 
			con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
			System.out.println(con.getWarnings());
		
		
			st = con.createStatement();
		
		
		//String table = "test_for_read";
		String c="create table if not exists test_hive(date string, value string) row format delimited fields terminated by ','";
		
		st.execute(c);
		System.out.println("Table created successfully");
		//load data inpath '/authoritative/output.csv' into table test_hive;
		String query= "load data inpath '/authoritative/output.csv' into table test_hive";
		int q= st.executeUpdate(query);
		System.out.println("Boolean q :"+q );
		
		/*
		String serdeClass = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
		String[] fieldnames = "date value".split("key value");
		HiveEndPoint hiveEP = new HiveEndPoint("thrift://localhost:10000", "default", "hive_test", null );
		//StreamingConnection connection= null;
		//DelimitedInputWriter writer = null;
		//con.addResource(new Path("/home/hadoop/hive/conf/hive-site.xml"));
		
		StreamingConnection connection = hiveEP.newConnection(false);
		DelimitedInputWriter writer = new DelimitedInputWriter(fieldnames, ",", hiveEP);
		TransactionBatch txnBatch = connection.fetchTransactionBatch(10, writer);
		
		//TransactionBatch txnBatch = connection.fetchTransactionBatch(10, writer);
		txnBatch.beginNextTransaction();
		txnBatch.write("1,Hello streaming".getBytes());
		txnBatch.write("2,Welcome to streaming".getBytes());
		txnBatch.commit();
		
		txnBatch.close();
		connection.close();*/
	}
	
	/*This method goes through each file in the files list and parse/filters the data that are requested.
	 * It writes the extracted data to the HDFS file and then call the loadtohive function to load that data to the hive table.*/
	public static void getdata(List<String> files, Time starttime,
			Time endtime, Configuration conf, String driverName) throws IOException, ClassNotFoundException,  InterruptedException {
		
		//List<String> files = new ArrayList<String>();
		byte[] data =null;
		
		FileSystem fs = FileSystem.get(conf);
		Path po= new Path("/authoritative/output.csv");
		FSDataOutputStream out = fs.create(po);
		
		 
		for(int k=0; k<files.size();k++)
		{
				Path p = new Path(files.get(k));
				
				FSDataInputStream file= fs.open(p);
				
				//BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(po,false)));
				data = IOUtils.toByteArray(new GZIPInputStream(file));
				file.read(data);
		
				System.out.println("data from file"+files.get(k));
		
				ByteBuffer b = ByteBuffer.wrap(data);
		
				int size=0,i=0;
		
				byte[] record=null;
				ArrayList<String> datapos = new ArrayList<String>();
				Rawlog rl = null;
		
				while(b.hasRemaining())
				{
			
					size = b.getInt();
			
					String s = String.valueOf(size)+","+String.valueOf(b.position());
					datapos.add(s);
					int pos = size+b.position();
			
					b.position(pos);
			
			
				}
				
			if(k==0)
			{
				System.out.println("data from file"+files.get(k));
				
				
				
				while(i<datapos.size())
				{
					
				String[] str = datapos.get(i).split(",");
				
				record = new byte[Integer.parseInt(str[0])];
				b.position(Integer.parseInt(str[1]));
				
				b.get(record,0,Integer.parseInt(str[0]));
	
				ByteBuffer br = ByteBuffer.wrap(record);
				rl = Rawlog.getRootAsrawLog(br);
				String[] logtime = rl.created().split(" ");
				String[] cur = logtime[1].split(":");
				System.out.println(logtime[0]+ " "+ logtime[1]);
			
				Time curtime = new Time(Integer.parseInt(cur[0]),Integer.parseInt(cur[1]),Integer.parseInt(cur[2]));
				String[] time=logtime[1].split(":");
				if( curtime.after(starttime) || curtime.equals(starttime))
				{
					
					out.writeBytes(logtime[0]+","+logtime[1]+"\n");
					System.out.println("This is the record: "+" "+rl.created());
				}
				
				i++;		
			
				}
			}
			else if(k==files.size()-1)
			{
					System.out.println("data from file"+files.get(k));
					
					while(i<datapos.size())
					{
						
					String[] str = datapos.get(i).split(",");
					
					record = new byte[Integer.parseInt(str[0])];
					b.position(Integer.parseInt(str[1]));
					
					b.get(record,0,Integer.parseInt(str[0]));
		
					ByteBuffer br = ByteBuffer.wrap(record);
					rl = Rawlog.getRootAsrawLog(br);
					String[] logtime = rl.created().split(" ");
					String[] cur = logtime[1].split(":");
					System.out.println(logtime[0]+ " "+ logtime[1]);
				
					Time curtime = new Time(Integer.parseInt(cur[0]),Integer.parseInt(cur[1]),Integer.parseInt(cur[2]));
					String[] time=logtime[1].split(":");
					System.out.println(curtime);
					if( curtime.before(endtime) || curtime.equals(endtime))
					{
						out.writeBytes(logtime[0]+","+logtime[1]+"\n");
						System.out.println("This is the record: "+" "+rl.created());
					}
					i++;		
				
					}
				
			}
			else{
			while(i<datapos.size())
			{
			
			String[] str = datapos.get(i).split(",");
			
			record = new byte[Integer.parseInt(str[0])];
			b.position(Integer.parseInt(str[1]));
			
			b.get(record,0,Integer.parseInt(str[0]));

			ByteBuffer br = ByteBuffer.wrap(record);
			rl = Rawlog.getRootAsrawLog(br);
			String[] logtime = rl.created().split(" ");
			String[] time=logtime[1].split(":");
			out.writeBytes(logtime[0]+","+logtime[1]+"\n");
			System.out.println("This is the record: "+" "+rl.created());
	
			i++;		
			}
			}
		}
		out.close();
		try {
			loaddatatohive(conf, driverName);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
	
	/*This method filter out the files which are needed form the folders that are generated in the folders list.
	 * Stores the filtered required filenames to the files arraylist.*/	
	
	
	public static List<String> getfiles(
			List<String> folders, List<String> patterns,
			FileSystem fs1) {
		ArrayList<String> file_not_found = new ArrayList<String>();
		List<String> files = new ArrayList<String>();
		for(String f: folders)
		{
			
			//System.out.println("In the get files");
			Path folder =new Path(f);
			
			FileStatus[] status;
			try {
				status = fs1.listStatus(folder);
				System.out.println("sttus : "+status.length);
				for(int p=0; p<status.length; p++ )
				{
					String file = status[p].getPath().getName();
					System.out.println("file is: "+file);
					for ( int s =0 ; s< patterns.size(); s++)
					{
						//System.out.println("pattern matching:"+file);
						if(file.matches(patterns.get(s)))
						{
							System.out.println("String matches: "+ file + "with "+ patterns.get(s));
							files.add(f+"/"+file);
							s= patterns.size();
						}
					}
					
				}
				System.out.println("file size :"+files.size());
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				file_not_found.add(folder.toString());
				System.out.println("File "+folder+"not found");
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			finally{
				System.out.println("These are the folders that were not found");
				for( int w=0; w<file_not_found.size(); w++)
				{
					System.out.println(file_not_found.get(w));
					
				}
			}
			
			
			
		}
		return files;
	}
	
	
	/*This method takes in the JSON parameters and generates the regex pattern to be used in the file parsing/filtering stage.
	 * Returns a list of all the possible regexes to match based on the JSON parameters passed to it.*/
	
	
	public static List<String> getpatterns( String revision_id,
			List<Integer> producer_id, String sequence_num,
			List<Integer> advertiser_id, List<Integer> prison_id) 
	{
		List<String> patterns = new ArrayList<String>();
		for ( int h = 0; h <advertiser_id.size(); h++)
	  {
		String shard_id= String.valueOf((advertiser_id.get(h))%21);
		System.out.println("Shard_id :"+shard_id);
		StringBuilder patternname= new StringBuilder();
		
		//TODO : Right now we are returning every revision which is less than the specified revision_id. A filtering needs to be done so that it returns files with the highest revision number.
			
		if( prison_id.size()==0  && producer_id.size()==0)
			{
				patternname.append("shard"+shard_id+"_\\d{8}_rev\\d{1,2}_prison\\d{2}_batcher\\d{2}_seq\\d{1,2}.fb.gz");
				System.out.println("Pattern: "+patternname.toString());
				patterns.add(patternname.toString());
			}
			else if(prison_id.size()==0 && producer_id.size()!=0)
			{	
				for(int i=0 ; i < producer_id.size(); i++)
				{
					String producer= String.format("%02d", producer_id.get(i)); 
					patternname.append("shard"+shard_id+"_\\d{8}_rev\\d{1,2}_prison\\d{2}_batcher"+producer+"_seq\\d{1,2}.fb.gz");
					System.out.println("Pattern: "+patternname.toString());
					patterns.add(patternname.toString());
				}
		   }
			else if( prison_id.size()!=0 && producer_id.size()==0)
			{
				for(int i=0 ; i < prison_id.size(); i++)
				{
					String prison= String.format("%02d", prison_id.get(i)); 
					patternname.append("shard"+shard_id+"_\\d{8}_rev\\d{1,2}_prison"+prison+"_batcher\\d{1,2}_seq\\d{1,2}.fb.gz");
					System.out.println("Pattern: "+patternname.toString());
					patterns.add(patternname.toString());
				}
			}
			else
			{
				for(int i=0 ; i < prison_id.size(); i++)
				{
					String prison= String.format("%02d", prison_id.get(i)); 
					for(int j=0 ; j < producer_id.size(); j++)
					{
						String producer= String.format("%02d", producer_id.get(j)); 
						patternname.append("shard"+shard_id+"_\\d{8}_rev\\d{1,2}_prison"+prison+"_batcher"+producer+"_seq\\d{1,2}.fb.gz");
						System.out.println("Pattern: "+patternname.toString());
						patterns.add(patternname.toString());	
					}
				}
			}
			patternname.setLength(0);
		}
		System.out.println(patterns.size());
		return patterns;
	}

	/*This method generates the folder names according to the start and end date 
	 * provided as MIN and MAX in the JSON.
	 * It creates the folder names that actually exists in the HDFS or s3 buckets. This folders will contain the file that are requested.
	 */
public static List<String> getfoldernames( Date startdate, Date enddate) {
		
		List<String> folders= new ArrayList<String>();
		List<Date> dates = new ArrayList<Date>();
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(startdate);
		 
		while( (calendar.getTime().before(enddate)) || calendar.getTime().equals(enddate))
		{
			Date resultdate = calendar.getTime();
			dates.add(resultdate);
			String foldername = "/authoritative/"+resultdate.getYear()+"/"+String.format("%02d",(resultdate.getMonth()+1))+"/"+String.format("%02d",resultdate.getDate());
			System.out.println(foldername);
			calendar.add(Calendar.DATE,1);
			folders.add(foldername);
		}
		return folders;
	}
		/*
		 * This is the initial code that I have written before using the java Calendar API. It was faster compared to the current approach of generating the foldernames.
		 * But because of the complexity I have commented it out.
		 * 
		for(int y=Integer.parseInt(from_year); y<=Integer.parseInt(from_year)+year_range ; y++)
		{
		
		int year = y;
		Calendar c= Calendar.getInstance();
		c.set(c.YEAR, year);
		if(year==Integer.parseInt(from_year) && year_range==0)
		{
		
		for(int i =Integer.parseInt(from_month); i<=Integer.parseInt(from_month)+month_range; i++)
		{
			
			int m=i;
			c.set(c.MONTH, m-1);
			int total_days= c.getActualMaximum(c.DAY_OF_MONTH);
			System.out.println("total_days :"+total_days+ " For month: "+ m);
			String month = String.format("%02d", m);
			if(m== Integer.parseInt(from_month) && month_range>0)
			{
			for ( int j=Integer.parseInt(from_day); j<=total_days;j++)
			{
				
				String day = String.format("%02d", j);
				StringBuilder folderpath = new StringBuilder();
				folderpath.append("/authoritative/"+year+"/"+month+"/"+day);
				System.out.println("File path 5555 here: "+folderpath.toString());
				folders.add(folderpath.toString());
						
			}
			}
			else if (m== Integer.parseInt(from_month) && month_range==0)
			{
				for(int j =0; j<=days_range; j++)
				{
					String day = String.format("%02d",Integer.parseInt(from_day)+j);
					StringBuilder folderpath = new StringBuilder();
					folderpath.append("/authoritative/"+year+"/"+month+"/"+day);
					System.out.println("File path 5555 here: "+folderpath.toString());
					folders.add(folderpath.toString());
					
				}
			}
			if (m == Integer.parseInt(from_month)+month_range && month_range>0)
			{
				for ( int j1 = 1; j1<=Integer.parseInt(from_day)+days_range; j1++)
				{
					String day = String.format("%02d", j1);
					StringBuilder folderpath = new StringBuilder();
					folderpath.append("/authoritative/"+year+"/"+month+"/"+day);
					System.out.println("File path 5555 here: "+folderpath.toString());
					folders.add(folderpath.toString());
				}
			}
			if( m >Integer.parseInt(from_month) && m< Integer.parseInt(from_month)+month_range)
			{
				for ( int j1 = 1; j1<=total_days; j1++)
				{
					String day = String.format("%02d", j1);
					StringBuilder folderpath = new StringBuilder();
					folderpath.append("/authoritative/"+year+"/"+month+"/"+day);
					System.out.println("File path 5555 here: "+folderpath.toString());
					folders.add(folderpath.toString());
				}
			}
					
		}
				
		}
		else if (year == Integer.parseInt(from_year) && year_range>0)
		{
			for( int  u= Integer.parseInt(from_month) ; u<=12; u++)
			{
				int m =u;
				c.set(c.MONTH, m-1);
				int total_days= c.getActualMaximum(c.DAY_OF_MONTH);
				String month = String.format("%02d", u);
				if( m == Integer.parseInt(from_month))
				{
					for ( int j=Integer.parseInt(from_day); j<=total_days;j++)
					{
						
						String day = String.format("%02d", j);
						StringBuilder folderpath = new StringBuilder();
						folderpath.append("/authoritative/"+year+"/"+month+"/"+day);
						System.out.println("File path 5555 here: "+folderpath.toString());
						folders.add(folderpath.toString());
								
					}
				}
				else{
					for ( int j1 = 1; j1<=total_days; j1++)
					{
						String day = String.format("%02d", j1);
						StringBuilder folderpath = new StringBuilder();
						folderpath.append("/authoritative/"+year+"/"+month+"/"+day);
						System.out.println("File path 5555 here: "+folderpath.toString());
						folders.add(folderpath.toString());
					}
				}public static ArrayList<Object> getobjects_buffer(byte[] byte_blob) throws IOException 
	{
		ByteBuffer byte_buffer = ByteBuffer.wrap(byte_blob);
		rawLog raw_log= rawLog.getRootAsrawLog(byte_buffer);
		
		return createList(raw_log);
	}

	private static ArrayList<Object> createList(rawLog rl) {
		
		
		ArrayList<Object> value_obj = new ArrayList<Object>();
		
		value_obj.add(rl.adNetworkId());
		value_obj.add(rl.adId());
		value_obj.add(rl.created());
		value_obj.add(rl.advertiserFileId());
		value_obj.add(rl.advertiserRefId());
		value_obj.add(rl.advertiserSubAdgroup());
		value_obj.add(rl.advertiserSubAd());
	
		
		return value_obj;
		// TODO Auto-generated method stub
		
	}
			}
		}
		else if( year == Integer.parseInt(from_year)+year_range && year_range>0)
		{
			for( int u=1; u<= Integer.parseInt(from_month)+month_range ; u++)
			{
				int m= u;
				c.set(c.MONTH, m-1);
				int total_days= c.getActualMaximum(c.DAY_OF_MONTH);
				String month = String.format("%02d", u);
				if(m== Integer.parseInt(from_month)+month_range && month_range>=0)
				{
					for ( int j1 = 1; j1<=Integer.parseInt(from_day)+days_range; j1++)
					{
						String day = String.format("%02d", j1);
						StringBuilder folderpath = new StringBuilder();
						folderpath.append("/authoritative/"+year+"/"+month+"/"+day);
						System.out.println("File path 5555 here: "+folderpath.toString());
						folders.add(folderpath.toString());
					}
				}
				else
				{
					for ( int j1 = 1; j1<=total_days; j1++)
					{
						String day = String.format("%02d", j1);
						StringBuilder folderpath = new StringBuilder();
						folderpath.append("/authoritative/"+year+"/"+month+"/"+day);
						System.out.println("File path 5555 here: "+folderpath.toString());
						folders.add(folderpath.toString());
					}
				}
			}
		}
		else{
			for( int  u=1 ; u<=12; u++)
			{
				int m=u;
				c.set(c.MONTH, m-1);
				int total_days= c.getActualMaximum(c.DAY_OF_MONTH);
				String month = String.format("%02d", u);
				for( int d=1; d<=total_days ; d++)
				{
					String day = String.format("%02d", d);
					StringBuilder folderpath = new StringBuilder();
					folderpath.append("/authoritative/"+year+"/"+month+"/"+day);
					System.out.println("File path 5555 here: "+folderpath.toString());
					folders.add(folderpath.toString());
				}
			}
		}
	}*/
		
	
	
	public static ArrayList<Object> getobjects(String filename , Configuration conf) throws IOException 
	{
		byte[] data = null;
 		FileSystem fs = FileSystem.get(conf);

		Path p = new Path(filename);
		FSDataInputStream file = fs.open(p);
		
		java.io.InputStream io = new GZIPInputStream(file);
		
		TuneFlatBufferRecordReader inter = new TuneFlatBufferRecordReader();
		inter.initialize(io, new Configuration(), new Properties());
		//int offset=0;
		for(int i= 0 ; i<40 ; i++)
		{	
			
			System.out.println("Initialized");
			BytesWritable r = (BytesWritable) inter.createRow();
			System.out.println("get byteswritable");
			int readedbytes = inter.next(r);
			r= inter.getBytes();
			System.out.println("inside the utility : "+ ((BytesWritable)r).getCapacity());
			System.out.print("number of bytes read: "+readedbytes);
			if(readedbytes >0)
			{
			List<Object> result = TuneFlatBufferRecordReader.getObjectBuffer(r);
			System.out.println("Reader size: "+ readedbytes);
			System.out.println("Result from the change: "+ result.get(0)+ " "+ result.get(2));
			
			}else
			{
				ArrayList<Object> afobj = new ArrayList<Object>();
				System.out.println("Reader size: "+ readedbytes);
				//System.out.println("Result from the change: "+ result.get(0)+ " "+ result.get(2));
				System.out.println("Reach the end of the file");
				return afobj;
			}
		//BytesWritable r = FileReader.readRow(io);
		//List<Object> result = Reader.getObjectBuffer(r);
		//offset= r.getSize()+4;
		
		}
		//file.close();
		return null;
	}

/*	
public static BytesWritable readRow(java.io.InputStream io) throws IOException
	
	{
		//FileDescriptor f =file.getFileDescriptor();
		//file.
		//System.out.println("File stream: "+file + "  "+ f);
		//GZIPInputStream file_input= null;
	
	
		//System.out.println("Method starts"+ offset + " "+ (offset+4));
		
		
		byte[] number = new byte[4];
		Integer readBytes = 0;
		while (4 - readBytes > 0) 
		{
			readBytes += io.read(number, readBytes, 4 - readBytes);
		}
		
		System.out.println(number.length);
		ByteBuffer bNumber = ByteBuffer.wrap(number);
		int position = bNumber.getInt();
		byte[] result = new byte[position];
		Integer readByte = 0;
		while (position - readByte > 0) 
		{
		 readByte += io.read(result, readByte , position- readByte);
		}
		System.out.println("offset: "+position);
		//input.close();
		//file.close();
	return new BytesWritable(result);
	
	
		
	}
*/
	

}

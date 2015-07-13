package play;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import MyGame.rawLog;

import com.twitter.chill.Base64.InputStream;

public class Utility {
	
	public static Properties readParameters(String path){
		Properties p = new Properties();
		java.io.InputStream in = null;
		try {
			in = new FileInputStream(path);
			p.load(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			if(in != null)
			{
				try {
					in.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return p;
		
	}
	
	
	public static void loaddatatohive(Configuration conf, String driverName) throws SQLException
	{
		Connection con=null;
		Statement st = null;
		
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 
		try {
			con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hadoop", "");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			st = con.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String table = "test_for_read";
		String c="create table if not exists test_hive(date string, value string) row format delimited fields terminated by ','";
		
		st.execute(c);
		System.out.println("Table created successfully");
		//load data inpath '/authoritative/output.csv' into table test_hive;
		String query= "load data inpath '/authoritative/output.csv' into table test_hive";
		int q= st.executeUpdate(query);
		System.out.println("Boolean q :"+q );
	}
	
	public static void getdata(ArrayList<String> files, Time starttime,
			Time endtime, Configuration conf, String driverName) throws IOException {
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
				rawLog rl = null;
		
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
				rl = rawLog.getRootAsrawLog(br);
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
					rl = rawLog.getRootAsrawLog(br);
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
			rl = rawLog.getRootAsrawLog(br);
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
		//ByteBuffer bb = ByteBuffer.wrap(data);
		 //System.out.println("starting jdbc conncetion");
		/*  Class.forName(driverName);  
		// System.out.println("Conncetion established.");
		 Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10001/default", "APP", "mine");
		// System.out.println("Conncetion established.");
		 java.sql.Statement st = con.createStatement();
		// System.out.println("Conncetion established.");
		 String table = "test_forread";
		 st.execute("create table "+ table +"if not exists (key int, value string)");
		 String sql1 = "show tables";
		 ResultSet rs = st.executeQuery(sql1);
		 if(rs.next())
		 {
			 System.out.println(rs.getString(1));
		 }
		 while(i<datapos.size())
				{
					System.out.println(i);
					System.out.println(datapos.size());
					String[] str = datapos.get(i).split(",");
					System.out.println(Integer.parseInt(str[0]));
					//System.out.println("Number: "+size);
					record = new byte[Integer.parseInt(str[0])];
					b.position(Integer.parseInt(str[1]));
					//System.out.println("length :"+record.length);
					b.get(record,0,Integer.parseInt(str[0]));
		
					ByteBuffer br = ByteBuffer.wrap(record);
					rl = rawLog.getRootAsrawLog(br);
					String v1 = rl.countryCode();
					int v2 = rl.countryId();
					//System.out.println(v1 +"  "+v2);
					String sql2 = "INSERT INTO TABLE "+ table+" VALUES ("+v2+","+ v1+")";
					System.out.println("This is the record: "+" "+rl.countryCode()+rl.countryId());
					i++;		
				}*/
		 //System.out.println("Ends");
	/*
	public static void getfiles(ArrayList<String> files,
			ArrayList<String> folders, ArrayList<String> patterns,
			FileSystem fs1) throws FileNotFoundException, IOException {
		for(String f: folders)
		{
			
			//System.out.println("In the get files");
			Path folder =new Path(f);
			
			FileStatus[] status = fs1.listStatus(folder);
			
			for(int p=0; p<status.length; p++ )
			{
				String file = status[p].getPath().getName();
				//System.out.println("file is: "+file);
				for ( int s =0 ; s< patterns.size(); s++)
				{
					System.out.println("pattern matching:"+file);
					if(file.matches(patterns.get(s)))
					{
						System.out.println("String matches: "+ file + "with "+ patterns.get(s));
						files.add(f+"/"+file);
						break;
					}
				}
				
			}
			
		}
	}
	*/
	public static void getfiles(ArrayList<String> files,
			ArrayList<String> folders, ArrayList<String> patterns,
			FileSystem fs1) {
		ArrayList<String> file_not_found = new ArrayList<String>();
		for(String f: folders)
		{
			
			//System.out.println("In the get files");
			Path folder =new Path(f);
			
			FileStatus[] status;
			try {
				status = fs1.listStatus(folder);
				for(int p=0; p<status.length; p++ )
				{
					String file = status[p].getPath().getName();
					//System.out.println("file is: "+file);
					for ( int s =0 ; s< patterns.size(); s++)
					{
						System.out.println("pattern matching:"+file);
						if(file.matches(patterns.get(s)))
						{
							System.out.println("String matches: "+ file + "with "+ patterns.get(s));
							files.add(f+"/"+file);
							break;
						}
					}
					
				}
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
	}
	
	public static void getpatterns(ArrayList<String> patterns , Date startdate, Date enddate,  String revision_id,
			ArrayList<Integer> producer_id, String sequence_num,
			ArrayList<Integer> advertiser_id, ArrayList<Integer> prison_id) {
		for ( int h = 0; h <advertiser_id.size(); h++)
		{
		String shard_id= String.valueOf((advertiser_id.get(h))%21);
		
		System.out.println("Shard_id :"+shard_id);
		StringBuilder patternname= new StringBuilder();
		
		
			//TODO : Right now we are returning every revision which is less than the specified revision_id. A filtering needs to be done so that it returns files with the highest revision number.
			
			
			patternname.append("shard"+shard_id+"_\\d{8}_rev\\d{1,2}");
			if( prison_id.size()==0  && producer_id.size()==0)
			{
				System.out.println("Inside first if");
				StringBuilder pattern = new StringBuilder(patternname.toString());
				
				pattern.append("_prison\\d{2}_batcher\\d{2}_seq\\d{1,2}.fb.gz");
				System.out.println("Pattern: "+pattern.toString());
				patterns.add(pattern.toString());
			}
			else if(prison_id.size()==0 && producer_id.size()!=0)
			{
				System.out.println("Inside second if");
			for(int i2=0 ; i2 < producer_id.size(); i2++)
			{
				
				StringBuilder pattern = new StringBuilder(patternname.toString());
				String producer= String.format("%02d", producer_id.get(i2)); 
				
				pattern.append("_prison\\d{2}_batcher"+producer+"_seq\\d{1,2}.fb.gz");
				System.out.println("Pattern: "+pattern.toString());
				patterns.add(pattern.toString());
				
			
			}
		   }
			else if( prison_id.size()!=0 && producer_id.size()==0)
			{
				System.out.println("Inside third if");
				for(int i2=0 ; i2 < prison_id.size(); i2++)
				{
			
					StringBuilder pattern = new StringBuilder(patternname.toString());
					String prison= String.format("%02d", prison_id.get(i2)); 
					
					pattern.append("_prison"+prison+"_batcher\\d{1,2}_seq\\d{1,2}.fb.gz");
					System.out.println("Pattern: "+pattern.toString());
					patterns.add(pattern.toString());
				
				}
			}
			else
			{
				System.out.println("Inside last if");
				for(int i2=0 ; i2 < prison_id.size(); i2++)
				{
					
					StringBuilder pattern = new StringBuilder(patternname.toString());
					String prison= String.format("%02d", prison_id.get(i2)); 
					for(int i3=0 ; i3 < producer_id.size(); i3++)
					{
						StringBuilder f_pattern = new StringBuilder(pattern.toString());
						String producer= String.format("%02d", producer_id.get(i3)); 
						
						f_pattern.append("_prison"+prison+"_batcher"+producer+"_seq\\d{1,2}.fb.gz");
						System.out.println("Pattern: "+f_pattern.toString());
						patterns.add(f_pattern.toString());	
					}
				}
			}
			patternname.setLength(0);
		
		
		}
		System.out.println(patterns.size());
	}

		
	public static void getfoldernames(ArrayList<String> folders, Date startdate, Date enddate) {
		
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
	
		/*
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
	}


}

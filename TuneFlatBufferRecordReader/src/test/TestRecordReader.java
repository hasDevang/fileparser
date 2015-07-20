package test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;



import com.tune.flatbufferreader.TuneFlatBufferRecordReader;

public class TestRecordReader {
	
	//static JSONObject jsonArray;
	public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException, InterruptedException
	{
		if(args.length != 0)
		{
			Path p = new Path(args[0]);
			Configuration conf= new Configuration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream file = fs.open(p);
			java.io.InputStream io = new GZIPInputStream(file);

			TuneFlatBufferRecordReader inter = new TuneFlatBufferRecordReader();
			inter.initialize(io, new Configuration(), new Properties());   // Initializes the filestream for the given file from hdfs.
			
			//TO DO: Here the number of iterations will be decided by the Hive Driver.
			for(int i= 0 ; i<40 ; i++)
			{	
				BytesWritable r = (BytesWritable) inter.createRow();   //Getting the blank WritableBytes from the createRow Method.
				int readedbytes = inter.next(r); // Reads the first Binary FlatBuffer and returns the number of bytes written(blob size).
				r= inter.getBytes();
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
					return;
				}
			}
			
			return;
		}
	
		
		System.out.println("Run this jar by following command: java -jar <jarname> <GZIP FlatBuffer local file path>");
		return;
	}

}

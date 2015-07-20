package com.tune.flatbufferreader;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.io.BytesWritable;

/*@ author: Devang-Summer Intern (DataFlow team- TUNE)
 * Class to read  flatbuffer object from byteswritable binary blob.
 *
		*/

public class Reader {

	/*
	 * @parameter BytesWritable blob: binary blob from the flatbuffer file
	 * @return List<object>: Which are the attribute values from the flatbuffer object returned as the list in sequence.
	 * 
	 * This method takes the flatbuffer binary blob and convert it into the rawlog object which is the original log object.
	 * Returns the list of all the values as a list of objects.
	 */
	public static List<Object> getObjectBuffer(BytesWritable blob) throws IOException 
	{
		ByteBuffer byte_buffer = ByteBuffer.wrap(blob.getBytes());
		Rawlog raw_log= Rawlog.getRootAsrawLog(byte_buffer);
		
		return createList(raw_log);
	}

	/*
	 * @ parameter Rawlog: Rawlog object created from the flatbuffer binary blob.
	 * @return List<0bject>: List of objects of all the values for the Rawlog object.
	 * 
	 * Method takes in the Rawlog object and then returns the list of all the values in sequence in object format.
	 */
	private static List<Object> createList(Rawlog rl) {
		
		List<Object> value_obj = new ArrayList<Object>();
		
		value_obj.add(rl.adNetworkId());
		value_obj.add(rl.adId());
		value_obj.add(rl.created());
		value_obj.add(rl.advertiserFileId());
		value_obj.add(rl.advertiserRefId());
		value_obj.add(rl.advertiserSubAdgroup());
		value_obj.add(rl.advertiserSubAd());
	
		
		return value_obj;
		
		
	}
}

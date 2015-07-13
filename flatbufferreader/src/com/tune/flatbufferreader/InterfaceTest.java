package com.tune.flatbufferreader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.RecordReader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

public class InterfaceTest implements RecordReader {

	private java.io.InputStream fileInputStream;
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
			
	}

	@Override
	public Writable createRow() throws IOException {
		// TODO Auto-generated method stub
		byte[] blobSize = new byte[4];
		Integer readBytes = 0;
		while (4 - readBytes > 0) 
		{
			readBytes += fileInputStream.read(blobSize, readBytes, 4 - readBytes);
		}
		ByteBuffer bsize = ByteBuffer.wrap(blobSize);
		int size = bsize.getInt();
		byte[] binaryBlob = new byte[size];
		Integer readByte = 0;
		while ( size - readByte > 0) 
		{
		 readByte += fileInputStream.read(binaryBlob, readByte , size- readByte);
		}
		System.out.println("offset: "+size);
	
	  return new BytesWritable(binaryBlob);
	
	}

	@Override
	public void initialize(InputStream arg0, Configuration arg1, Properties arg2)
			throws IOException {
		// TODO Auto-generated method stub
		
		this.fileInputStream = arg0;
		
	}

	@Override
	public int next(Writable arg0) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}
	
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

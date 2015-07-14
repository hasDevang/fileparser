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

/**
 * 
 * @author devang
 * 
 * implements RecordReader interface org.apache.hadoop.hive.ql.exec.RecordReader.
 *
 */
public class TuneFlatBufferRecordReader implements RecordReader {

	private java.io.InputStream fileInputStream;
	private BytesWritable bytes;
	//private int maxRecordLength;
	
	@Override
	public void close() throws IOException {
		
			if( fileInputStream != null)
			{
				fileInputStream.close();
			}
	}

	/**
	 * Returns a blank ByteWritable. Assigning it to the private BytesWritable.
	 */
	@Override
	public Writable createRow() throws IOException {
		
		bytes = new BytesWritable();
		//bytes.setCapacity(maxRecordLength);
		return bytes;
	
	}
	/**
	 * Specifies the FlatBuffer file stream to read the blob from.
	 */

	@Override
	public void initialize(InputStream arg0, Configuration arg1, Properties arg2)
			throws IOException {
		
		this.fileInputStream = arg0;
		
	}

	/**
	 * @ parameter Writable
	 * Write one FlatBuffer binary blob from the FlatBuffer file.
	 * Returns int representing the number of bytes read. 
	 */
	@Override
	public int next(Writable r) throws IOException {
		
		
		byte[] blobSize = new byte[4];
		Integer readBytes = 0;
		while (4 - readBytes > 0) 
		{
			readBytes += fileInputStream.read(blobSize, readBytes, 4 - readBytes);
		}
		//ByteBuffer bsize = ByteBuffer.wrap(blobSize);
		int size = ByteBuffer.wrap(blobSize).getInt();
		byte[] binaryBlob = new byte[size];
		Integer readByte = 0;
		while ( size - readByte > 0) 
		{
		 readByte += fileInputStream.read(binaryBlob, readByte , size- readByte);
		}
		
		bytes = new BytesWritable(binaryBlob);
		return bytes.getSize();
	}
	
	/**
	 * getter method for the FileInputStream
	 * @return
	 */
	
	public java.io.InputStream getFileInputStream() {
		return fileInputStream;
	}

	/**
	 * Getter method for the private BytesWritable bytes;
	 * @return
	 */
	public BytesWritable getBytes() {
		return bytes;
	}

	/*
	 * @parameter BytesWritable blob: binary blob from the flatbuffer file
	 * @return List<object>: Which are the attribute values from the flatbuffer object returned as the list in sequence.
	 * 
	 * This method takes the flatbuffer binary blob and convert it into the rawlog object which is the original log object.
	 * Returns the list of all the values as a list of objects.
	 */

	public static List<Object> getObjectBuffer(BytesWritable blob) throws IOException 
	{
		System.out.println("in the getobjectbuffer :"+ blob.getSize());
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

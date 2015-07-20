package com.tune.flatbufferreader;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.BytesWritable;

/**
 * 
 * @author devang ,fuat
 * 
 *
 */
public class FileReader {

	/**
	 * 
	 * @param fileInputStream : InputStream to the file in HDFS
	 * @return BytesWritable : Returns ByteWritable Binary blob which will be used by flatbuffer reader.
	 * @throws IOException
	 */
public static BytesWritable readRow(java.io.InputStream fileInputStream) throws IOException
	
	{	
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

}

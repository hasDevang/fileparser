package tune.schema;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.io.*;

import org.apache.commons.io.IOUtils;





import org.apache.hadoop.fs.FSDataInputStream;

import tune.schema.rawLog;
//import com.google.flatbuffers.FlatBufferBuilder;

public class rowArray{
	
	
   	static int initial_capacity ;
   	static int position ;
   	static int anotherCapacity ;
    public static void main(String[] args) throws FileNotFoundException, IOException{
            byte[] data = null;
        //File file = new File("/home/fuat/Documentation/flatBuffer/rawLog2015032014.ral0");


               // byte[] data = null;
                //InputStream file =  new InputStream( new BufferReader(new File("/home/fuat/2015/03/21/shard14_20150321_rev0_prison00_batcher00_seq0.fb.gz")));
                DataInputStream stream = new DataInputStream(
                	   new BufferedInputStream(new FileInputStream(new File("/home/fuat/Documentation/flatBuffer/rawLog"))));
                
                
                //data = IOUtils.toByteArray(new GZIPInputStream( new FSDataInputStream("/home/fuat/2015/03/21/shard14_20150321_rev0_prison00_batcher00_seq0.fb.gz")));
               data= IOUtils.toByteArray(stream);
                
                //data = ByteStreams.toByteArray(file);
              /* System.out.println(data);

                ByteBuffer bb = ByteBuffer.wrap(data);

                while (bb.hasRemaining()) {
                	int size = bb.getInt(); 
                	byte[] record = new byte[size];
                	bb.get(record);
                    printData(record);
                   

                }*/
               
               int length = data.length;
               System.out.println(length);
               
               System.out.println(data.getClass().getName());
               //System.out.println(data[3]);
               byte[] offset = Arrays.copyOfRange(data, 0, 4);
               ByteBuffer bff = ByteBuffer.wrap(offset);
               System.out.println(bff.getInt());
               System.out.println(offset.length);
               byte[] readBuffer = Arrays.copyOfRange(data, 4, 1448);
               //ByteBuffer bff2 = ByteBuffer.wrap(readBuffer);
               printData(readBuffer);
               
               //System.out.println(bff2.getInt());
              // data = IOUtils.toByteArray(new GZIPInputStream( new FSDataInoutStream(file)));
              // byte[] newData = Arrays.copyOfRange(data, 0,100);// data.length);

              ByteBuffer buf = ByteBuffer.wrap(data);
              initial_capacity = buf.capacity();
             	position = buf.position();
             	anotherCapacity = initial_capacity;
               byte[] br = blobReader( buf,anotherCapacity);
               printData(br);
               byte[] br1 = blobReader( buf,anotherCapacity);
               printData(br1);
               byte[] br2 = blobReader( buf,anotherCapacity);
               printData(br2);
           	
          		
           	

}

    
    
    
 	
   	private static byte[] blobReader(ByteBuffer buf, int anotherCapacity) throws IOException {

   		byte[] record = null;
   		byte[] data = null;
   		int size = 0; 
   		
   		if (buf.hasRemaining()) {
   			int size1 = buf.getInt(); 
        	record = new byte[size1];
        	buf.get(record);
   			size = buf.getInt();
   			position = size+buf.position();
   			buf.position(position);
   			//record = new byte[size];
   			

   		}
   		anotherCapacity = anotherCapacity - size - 4;
   		return record;
   		
   	}
	private static void printData(byte[] record) {
		
			ByteBuffer bb = ByteBuffer.wrap(record);
			rawLog rlog = tune.schema.rawLog.getRootAsrawLog(bb);
			System.out.println(rlog.advertiserId()+"  "+rlog.created()+" "+ rlog.clickCreated()+"  "+rlog.advertiserSubAdgroup()+" "+rlog.attributedIdDate());
			
			
	}
}

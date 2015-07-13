package tune.schema;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.io.*;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.jets3t.service.io.GZipInflatingOutputStream;

import tune.schema.rawLog;

//import com.google.flatbuffers.FlatBufferBuilder;

public class rowArray2 {

	static int initial_capacity;
	static int position;
	static int anotherCapacity;
	static int arrayLength;
	static int lengthTracker = 0;

	public static void main(String[] args) throws FileNotFoundException, IOException{
            byte[] data = null;
        File file = new File("/home/fuat/2015/03/21/shard14_20150321_rev0_prison00_batcher00_seq0.fb.gz");


               // byte[] data = null;
                //InputStream file =  new InputStream( new BufferReader(new File("/home/fuat/2015/03/21/shard14_20150321_rev0_prison00_batcher00_seq0.fb.gz")));
               // DataInputStream stream = new DataInputStream(
                	//   new BufferedInputStream(new FileInputStream(new File("/home/fuat/Documentation/flatBuffer/rawLog"))));
                
               
                
                
                GZIPInputStream stream =  new GZIPInputStream(new FileInputStream(file));
                
                //data = IOUtils.toByteArray(new GZIPInputStream( new FSDataInputStream("/home/fuat/2015/03/21/shard14_20150321_rev0_prison00_batcher00_seq0.fb.gz")));
            for(int i = 0; i < 2 ; i++){   
               byte[] bs = new byte[4];
              // int offset3 = stream.readInt();
              // int c;
               int data1 = stream.read(bs, 0, 4);
               
               ByteBuffer bff2 = ByteBuffer.wrap(bs);
              // System.out.println(bff2.getInt());
               //int i = stream.read(bs);
               //System.out.println(data1+ "  " +i);
               int position1 = bff2.getInt() ;
               //int bufferSize = java.nio.ByteBuffer.wrap(bs).getShort();
              // System.out.println(bufferSize);
               System.out.println(position1);
               byte[] bs2 = new byte[position1];
               
               int data2 = stream.read(bs2,0, position1 );
               
               //ByteBuffer bff3 = ByteBuffer.wrap(bs2);
               printData(bs2);
            }   
              /* 
               for(byte b:bs)
               {
                  // converts byte to int
                  c=(int)b;
                  if(b==0)
                     c='-';
                  
                  // print
                  System.out.println(c);
               } */
               
               //System.out.println(data1);
             //  System.out.println(stream.read());
              // byte[] readBuffer3 = Arrays.copyOf(stream, 4, offset3+4);
                //System.out.println(50331716/8);
              //  System.out.println(stream.readInt());
               // stream.read(stream, 0, 4);
                
             //   data= IOUtils.toByteArray(stream);
                
                //data = ByteStreams.toByteArray(file);
              /* System.out.println(data);

                ByteBuffer bb = ByteBuffer.wrap(data);

                while (bb.hasRemaining()) {
                	int size = bb.getInt(); 
                	byte[] record = new byte[size];
                	bb.get(record);
                    printData(record);
                   

                }*/
                
   
                
    
             
             //  arrayLength = data.length;
               //System.out.println(arrayLength);
               
               //System.out.println(data.getClass().getName());
               //System.out.println(data[3]);
               /*byte[] offset = Arrays.copyOfRange(data, 0, 4);
               ByteBuffer bff = ByteBuffer.wrap(offset);
               int offset1 = bff.getInt();
               System.out.println(offset1);
               
               System.out.println(offset.length);
               byte[] readBuffer = Arrays.copyOfRange(data, 4, offset1);
               //ByteBuffer bff2 = ByteBuffer.wrap(readBuffer);
               printData(readBuffer);
               */
             /* int ll = data.length;
               while(ll> 0){
            	   byte[] offset = Arrays.copyOfRange(data, 0, 4);
            	   ByteBuffer bff2 = ByteBuffer.wrap(offset);
            	   int bufferLength = bff2.getInt();
            	   ll = data.length;
            	   try{
            	   byte[] readBuffer2 = Arrays.copyOfRange(data, 4, bufferLength);
            	   printData(readBuffer2);
            	   lengthTracker = 4+bufferLength;
            	   //System.out.println(bufferLength);
            	   //System.out.println(lengthTracker);
            	   data = Arrays.copyOfRange(data, 4+bufferLength, arrayLength);
            	   
            	   
            	   }catch(IllegalArgumentException e) {
            		   
            	   }
               }*/
            

             
           	
          		
           	

}

	private static void printData(byte[] record) {

		ByteBuffer bb = ByteBuffer.wrap(record);
		rawLog rlog = tune.schema.rawLog.getRootAsrawLog(bb);
		System.out.println(rlog.advertiserId() + "  " + rlog.created() + " "
				+ rlog.clickCreated() + "  " + rlog.advertiserSubAdgroup()
				+ " " + rlog.attributedIdDate());

	}
}

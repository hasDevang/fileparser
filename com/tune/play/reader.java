package play;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import MyGame.Monster;
import MyGame.TestFBS;
import MyGame.Vec3;

public class reader {

	public static void main (String args[])
	{
		byte[] data = null;
		File file = new File("/home/devang/outputfile");
		RandomAccessFile f = null;
		try{
			f = new RandomAccessFile(file, "r");
			data = new byte[(int)f.length()];
			f.readFully(data);
			f.close();
		
		}
		catch (Exception e)
		{
			System.out.println("error occured");
		}
		
		ByteBuffer bb = ByteBuffer.wrap(data);
		TestFBS tfbs = TestFBS.getRootAsTestFBS(bb);
		
		System.out.println("Advertiser_id :"+ tfbs.advertiserId());
		System.out.println("Advertiser_name :"+ tfbs.advertiserName());
		System.out.println("App_Name :"+ tfbs.appName());
		System.out.println("Attributable_type :"+ tfbs.attributableType());
		/*
		byte[] data = null;
        File file = new File("/home/devang/TestFBS_java_wire.tst");
        RandomAccessFile f = null;
        try {
            f = new RandomAccessFile(file, "r");
            data = new byte[(int)f.length()];
            f.readFully(data);
            f.close();
        } catch(java.io.IOException e) {
            System.out.println("FlatBuffers test: couldn't read file");
            return;
        }

        ByteBuffer bb = ByteBuffer.wrap(data);

        TestFBS tfbs = TestFBS.getRootAsTestFBS(bb);

        System.out.println(tfbs.advertiserName());
        System.out.println(tfbs.advertiserSubKeyword());
        System.out.println(tfbs.advertiserId());*/
	}
}

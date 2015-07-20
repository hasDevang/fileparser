package tune.schema;

import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.io.*;

import tune.schema.rawLog;

/**
 * 
 * @author fuat
 *
 */
public class rowArray2 {

	public static void main(String[] args) throws FileNotFoundException,
			IOException {
		String directory = "/home/fuat/2015/03/21/shard14_20150321_rev0_prison00_batcher00_seq0.fb.gz";
		readStreamBufferByBuffer(directory);

	}

	/**
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	private static void readStreamBufferByBuffer(String fileDirectory) throws IOException,
			FileNotFoundException {
		File file = new File(fileDirectory);

		@SuppressWarnings("resource")
		InputStream stream = new GZIPInputStream(new FileInputStream(file));

		int i = 0;

		while (true) {

			if (stream.available() <= 0)
				System.exit(0);
			i++;
			byte[] bs = new byte[4];

			Integer readBytes = 0;
			while (4 - readBytes > 0) {
				readBytes += stream.read(bs, readBytes, 4 - readBytes);
			}

			ByteBuffer bff2 = ByteBuffer.wrap(bs);
			int position1 = bff2.getInt();
			//System.out.println(position1);

			byte[] bs2 = new byte[position1];

			Integer bytesRead = 0;
			while (position1 - bytesRead > 0) {
				bytesRead += stream.read(bs2, bytesRead, position1 - bytesRead);
			}

			printData(bs2);

		}
		//System.out.println("row no:" + (i + 1));
	}

	/**
	 * 
	 * @param record
	 */
	private static void printData(byte[] record) {

		ByteBuffer bb = ByteBuffer.wrap(record);

		rawLog rlog = tune.schema.rawLog.getRootAsrawLog(bb);
		System.out.println(rlog.advertiserId() + "  " + rlog.created() + " "
				+ rlog.clickCreated() + "  " + rlog.advertiserSubAdgroup()
				+ " " + rlog.attributedIdDate());

	}
}

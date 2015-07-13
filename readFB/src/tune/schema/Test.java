package tune.schema;

import java.io.*;
import java.nio.ByteBuffer;

import tune.schema.rawLog;
import google.flatbuffers.FlatBufferBuilder;

public class Test{
    public static void main(String[] args){
        FlatBufferBuilder fbb = new FlatBufferBuilder();
        int str = fbb.createString("arslan");
        int name = fbb.createString("fuat");
        
        rawLog.startrawLog(fbb);///startTestFBS(fbb);
        rawLog.addAdvertiserId(fbb, 30);
        rawLog.addAdvertiserSubKeyword(fbb, str);
       // rawLog.addAdvertiserName(fbb, name);
        int finish_offset = rawLog.endrawLog(fbb);//endTestFBS(fbb);

        rawLog.finishrawLogBuffer(fbb, finish_offset);//finishTestFBSBuffer(fbb, finish_offset);
        try {
            DataOutputStream os = new DataOutputStream(new FileOutputStream(
                                           "/home/fuat/Documentation/flatBuffer/rowLog_java_fb.tst"));
            os.write(fbb.dataBuffer().array(), fbb.dataBuffer().position(), fbb.offset());
            os.close();
        } catch(java.io.IOException e) {
            System.out.println("FlatBuffers test: couldn't write file");
            return;
        }

    }

}

// automatically generated, do not modify

package MyGame;

import java.nio.*;
import java.lang.*;
import java.util.*;

import flatbuffers.*;

import flatbuffers.FlatBufferBuilder;
import flatbuffers.Table;

public final class TestFBS extends Table {
  public static TestFBS getRootAsTestFBS(ByteBuffer _bb) { return getRootAsTestFBS(_bb, new TestFBS()); }
  public static TestFBS getRootAsTestFBS(ByteBuffer _bb, TestFBS obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public TestFBS __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public String advertiserName() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer advertiserNameAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public int advertiserId() { int o = __offset(6); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String appName() { int o = __offset(8); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer appNameAsByteBuffer() { return __vector_as_bytebuffer(8, 1); }
  public String attributableType() { int o = __offset(10); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer attributableTypeAsByteBuffer() { return __vector_as_bytebuffer(10, 1); }

  public static int createTestFBS(FlatBufferBuilder builder,
      int advertiser_name,
      int advertiser_id,
      int app_name,
      int attributable_type) {
    builder.startObject(4);
    TestFBS.addAttributableType(builder, attributable_type);
    TestFBS.addAppName(builder, app_name);
    TestFBS.addAdvertiserId(builder, advertiser_id);
    TestFBS.addAdvertiserName(builder, advertiser_name);
    return TestFBS.endTestFBS(builder);
  }

  public static void startTestFBS(FlatBufferBuilder builder) { builder.startObject(4); }
  public static void addAdvertiserName(FlatBufferBuilder builder, int advertiserNameOffset) { builder.addOffset(0, advertiserNameOffset, 0); }
  public static void addAdvertiserId(FlatBufferBuilder builder, int advertiserId) { builder.addInt(1, advertiserId, 0); }
  public static void addAppName(FlatBufferBuilder builder, int appNameOffset) { builder.addOffset(2, appNameOffset, 0); }
  public static void addAttributableType(FlatBufferBuilder builder, int attributableTypeOffset) { builder.addOffset(3, attributableTypeOffset, 0); }
  public static int endTestFBS(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishTestFBSBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
};


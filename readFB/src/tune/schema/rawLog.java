package tune.schema;

// automatically generated, do not modify

import java.nio.*;
import java.lang.*;
import java.util.*;
import google.flatbuffers.*;

public final class rawLog extends Table {
  public static rawLog getRootAsrawLog(ByteBuffer _bb) { return getRootAsrawLog(_bb, new rawLog()); }
  public static rawLog getRootAsrawLog(ByteBuffer _bb, rawLog obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public static boolean rawLogBufferHasIdentifier(ByteBuffer _bb) { return __has_identifier(_bb, "DATS"); }
  public rawLog __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public String adId() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer adIdAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public int adNetworkId() { int o = __offset(6); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int advertiserFileId() { int o = __offset(8); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int advertiserId() { int o = __offset(10); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String advertiserRefId() { int o = __offset(12); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer advertiserRefIdAsByteBuffer() { return __vector_as_bytebuffer(12, 1); }
  public String advertiserSubAd() { int o = __offset(14); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer advertiserSubAdAsByteBuffer() { return __vector_as_bytebuffer(14, 1); }
  public String advertiserSubAdId() { int o = __offset(16); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer advertiserSubAdIdAsByteBuffer() { return __vector_as_bytebuffer(16, 1); }
  public String advertiserSubAdgroup() { int o = __offset(18); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer advertiserSubAdgroupAsByteBuffer() { return __vector_as_bytebuffer(18, 1); }
  public String advertiserSubAdgroupId() { int o = __offset(20); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer advertiserSubAdgroupIdAsByteBuffer() { return __vector_as_bytebuffer(20, 1); }
  public String advertiserSubCampaign() { int o = __offset(22); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer advertiserSubCampaignAsByteBuffer() { return __vector_as_bytebuffer(22, 1); }
  public String advertiserSubCampaignId() { int o = __offset(24); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer advertiserSubCampaignIdAsByteBuffer() { return __vector_as_bytebuffer(24, 1); }
  public String advertiserSubKeyword() { int o = __offset(26); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer advertiserSubKeywordAsByteBuffer() { return __vector_as_bytebuffer(26, 1); }
  public String advertiserSubKeywordId() { int o = __offset(28); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer advertiserSubKeywordIdAsByteBuffer() { return __vector_as_bytebuffer(28, 1); }
  public String advertiserSubPlacement() { int o = __offset(30); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer advertiserSubPlacementAsByteBuffer() { return __vector_as_bytebuffer(30, 1); }
  public String advertiserSubPlacementId() { int o = __offset(32); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer advertiserSubPlacementIdAsByteBuffer() { return __vector_as_bytebuffer(32, 1); }
  public String advertiserSubPublisher() { int o = __offset(34); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer advertiserSubPublisherAsByteBuffer() { return __vector_as_bytebuffer(34, 1); }
  public String advertiserSubPublisherId() { int o = __offset(36); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer advertiserSubPublisherIdAsByteBuffer() { return __vector_as_bytebuffer(36, 1); }
  public String advertiserSubSite() { int o = __offset(38); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer advertiserSubSiteAsByteBuffer() { return __vector_as_bytebuffer(38, 1); }
  public String advertiserSubSiteId() { int o = __offset(40); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer advertiserSubSiteIdAsByteBuffer() { return __vector_as_bytebuffer(40, 1); }
  public int age() { int o = __offset(42); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int agencyId() { int o = __offset(44); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public double altitude() { int o = __offset(46); return o != 0 ? bb.getDouble(o + bb_pos) : 0; }
  public String appName() { int o = __offset(48); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer appNameAsByteBuffer() { return __vector_as_bytebuffer(48, 1); }
  public String appVersion() { int o = __offset(50); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer appVersionAsByteBuffer() { return __vector_as_bytebuffer(50, 1); }
  public String attributableCreated() { int o = __offset(52); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer attributableCreatedAsByteBuffer() { return __vector_as_bytebuffer(52, 1); }
  public String attributableDateHour() { int o = __offset(54); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer attributableDateHourAsByteBuffer() { return __vector_as_bytebuffer(54, 1); }
  public String attributableId() { int o = __offset(56); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer attributableIdAsByteBuffer() { return __vector_as_bytebuffer(56, 1); }
  public String attributableIdDate() { int o = __offset(58); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer attributableIdDateAsByteBuffer() { return __vector_as_bytebuffer(58, 1); }
  public long attributableIdUnique1() { int o = __offset(60); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public long attributableIdUnique2() { int o = __offset(62); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public int attributableIntervalDay() { int o = __offset(64); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int attributableIntervalMonth() { int o = __offset(66); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int attributableIntervalWeek() { int o = __offset(68); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String attributableIntervalYear() { int o = __offset(70); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer attributableIntervalYearAsByteBuffer() { return __vector_as_bytebuffer(70, 1); }
  public String attributableType() { int o = __offset(72); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer attributableTypeAsByteBuffer() { return __vector_as_bytebuffer(72, 1); }
  public boolean attributableViewThrough() { int o = __offset(74); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public String attributeSub1() { int o = __offset(76); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer attributeSub1AsByteBuffer() { return __vector_as_bytebuffer(76, 1); }
  public String attributeSub2() { int o = __offset(78); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer attributeSub2AsByteBuffer() { return __vector_as_bytebuffer(78, 1); }
  public String attributeSub3() { int o = __offset(80); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer attributeSub3AsByteBuffer() { return __vector_as_bytebuffer(80, 1); }
  public String attributeSub4() { int o = __offset(82); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer attributeSub4AsByteBuffer() { return __vector_as_bytebuffer(82, 1); }
  public String attributeSub5() { int o = __offset(84); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer attributeSub5AsByteBuffer() { return __vector_as_bytebuffer(84, 1); }
  public String attributed() { int o = __offset(86); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer attributedAsByteBuffer() { return __vector_as_bytebuffer(86, 1); }
  public String attributedAgencyId() { int o = __offset(88); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer attributedAgencyIdAsByteBuffer() { return __vector_as_bytebuffer(88, 1); }
  public String attributedCreated() { int o = __offset(90); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer attributedCreatedAsByteBuffer() { return __vector_as_bytebuffer(90, 1); }
  public String attributedId() { int o = __offset(92); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer attributedIdAsByteBuffer() { return __vector_as_bytebuffer(92, 1); }
  public int attributedIdDate() { int o = __offset(94); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public long attributedIdUnique1() { int o = __offset(96); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public long attributedIdUnique2() { int o = __offset(98); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public String attributedPublisherId() { int o = __offset(100); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer attributedPublisherIdAsByteBuffer() { return __vector_as_bytebuffer(100, 1); }
  public String attributedType() { int o = __offset(102); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer attributedTypeAsByteBuffer() { return __vector_as_bytebuffer(102, 1); }
  public String attributionMethod() { int o = __offset(104); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer attributionMethodAsByteBuffer() { return __vector_as_bytebuffer(104, 1); }
  public float attributionPercent() { int o = __offset(106); return o != 0 ? bb.getFloat(o + bb_pos) : 0; }
  public float attributionPrecent() { int o = __offset(108); return o != 0 ? bb.getFloat(o + bb_pos) : 0; }
  public int campaignAdId() { int o = __offset(110); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public long campaignId() { int o = __offset(112); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public int campaignPayoutId() { int o = __offset(114); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int campaignUrlId() { int o = __offset(116); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String candidateClickIds() { int o = __offset(118); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer candidateClickIdsAsByteBuffer() { return __vector_as_bytebuffer(118, 1); }
  public String candidateImpressionIds() { int o = __offset(120); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer candidateImpressionIdsAsByteBuffer() { return __vector_as_bytebuffer(120, 1); }
  public String carrierAid() { int o = __offset(122); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer carrierAidAsByteBuffer() { return __vector_as_bytebuffer(122, 1); }
  public int channelId() { int o = __offset(124); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String clickCreated() { int o = __offset(126); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer clickCreatedAsByteBuffer() { return __vector_as_bytebuffer(126, 1); }
  public int clickGrossCount() { int o = __offset(128); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String clickId() { int o = __offset(130); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer clickIdAsByteBuffer() { return __vector_as_bytebuffer(130, 1); }
  public int clickIdDate() { int o = __offset(132); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public long clickIdUnique1() { int o = __offset(134); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public long clickIdUnique2() { int o = __offset(136); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public String clickMatchType() { int o = __offset(138); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer clickMatchTypeAsByteBuffer() { return __vector_as_bytebuffer(138, 1); }
  public int clickPublishersCount() { int o = __offset(140); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int clickUniqueCount() { int o = __offset(142); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String clickUrl() { int o = __offset(144); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer clickUrlAsByteBuffer() { return __vector_as_bytebuffer(144, 1); }
  public int clicks() { int o = __offset(146); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String connectionSpeed() { int o = __offset(148); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer connectionSpeedAsByteBuffer() { return __vector_as_bytebuffer(148, 1); }
  public String contentId() { int o = __offset(150); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer contentIdAsByteBuffer() { return __vector_as_bytebuffer(150, 1); }
  public String contentType() { int o = __offset(152); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer contentTypeAsByteBuffer() { return __vector_as_bytebuffer(152, 1); }
  public String conversionCreated() { int o = __offset(154); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer conversionCreatedAsByteBuffer() { return __vector_as_bytebuffer(154, 1); }
  public String conversionDateHour() { int o = __offset(156); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer conversionDateHourAsByteBuffer() { return __vector_as_bytebuffer(156, 1); }
  public String conversionId() { int o = __offset(158); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer conversionIdAsByteBuffer() { return __vector_as_bytebuffer(158, 1); }
  public String conversionIdDate() { int o = __offset(160); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer conversionIdDateAsByteBuffer() { return __vector_as_bytebuffer(160, 1); }
  public long conversionIdUnique1() { int o = __offset(162); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public long conversionIdUnique2() { int o = __offset(164); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public String conversionStatus() { int o = __offset(166); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer conversionStatusAsByteBuffer() { return __vector_as_bytebuffer(166, 1); }
  public String conversionType() { int o = __offset(168); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer conversionTypeAsByteBuffer() { return __vector_as_bytebuffer(168, 1); }
  public float cost() { int o = __offset(170); return o != 0 ? bb.getFloat(o + bb_pos) : 0; }
  public String costModel() { int o = __offset(172); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer costModelAsByteBuffer() { return __vector_as_bytebuffer(172, 1); }
  public String costTargetEventId() { int o = __offset(174); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer costTargetEventIdAsByteBuffer() { return __vector_as_bytebuffer(174, 1); }
  public String costTargetEventType() { int o = __offset(176); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer costTargetEventTypeAsByteBuffer() { return __vector_as_bytebuffer(176, 1); }
  public float costs() { int o = __offset(178); return o != 0 ? bb.getFloat(o + bb_pos) : 0; }
  public String countryCode() { int o = __offset(180); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer countryCodeAsByteBuffer() { return __vector_as_bytebuffer(180, 1); }
  public int countryId() { int o = __offset(182); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String created() { int o = __offset(184); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer createdAsByteBuffer() { return __vector_as_bytebuffer(184, 1); }
  public String createdDate() { int o = __offset(186); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer createdDateAsByteBuffer() { return __vector_as_bytebuffer(186, 1); }
  public String currencyCode() { int o = __offset(188); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer currencyCodeAsByteBuffer() { return __vector_as_bytebuffer(188, 1); }
  public String date1() { int o = __offset(190); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer date1AsByteBuffer() { return __vector_as_bytebuffer(190, 1); }
  public String date2() { int o = __offset(192); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer date2AsByteBuffer() { return __vector_as_bytebuffer(192, 1); }
  public boolean debugMode() { int o = __offset(194); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public String deviceBrand() { int o = __offset(196); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer deviceBrandAsByteBuffer() { return __vector_as_bytebuffer(196, 1); }
  public String deviceCarrier() { int o = __offset(198); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer deviceCarrierAsByteBuffer() { return __vector_as_bytebuffer(198, 1); }
  public String deviceId() { int o = __offset(200); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer deviceIdAsByteBuffer() { return __vector_as_bytebuffer(200, 1); }
  public String deviceIdMd5() { int o = __offset(202); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer deviceIdMd5AsByteBuffer() { return __vector_as_bytebuffer(202, 1); }
  public String deviceIdSha1() { int o = __offset(204); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer deviceIdSha1AsByteBuffer() { return __vector_as_bytebuffer(204, 1); }
  public String deviceIdSha256() { int o = __offset(206); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer deviceIdSha256AsByteBuffer() { return __vector_as_bytebuffer(206, 1); }
  public String deviceIp() { int o = __offset(208); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer deviceIpAsByteBuffer() { return __vector_as_bytebuffer(208, 1); }
  public String deviceIpHash() { int o = __offset(210); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer deviceIpHashAsByteBuffer() { return __vector_as_bytebuffer(210, 1); }
  public String deviceModel() { int o = __offset(212); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer deviceModelAsByteBuffer() { return __vector_as_bytebuffer(212, 1); }
  public String deviceType() { int o = __offset(214); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer deviceTypeAsByteBuffer() { return __vector_as_bytebuffer(214, 1); }
  public String eventItems() { int o = __offset(216); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer eventItemsAsByteBuffer() { return __vector_as_bytebuffer(216, 1); }
  public String eventType() { int o = __offset(218); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer eventTypeAsByteBuffer() { return __vector_as_bytebuffer(218, 1); }
  public int events() { int o = __offset(220); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public boolean existingUser() { int o = __offset(222); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public boolean fbAttribution() { int o = __offset(224); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public String fbCookie() { int o = __offset(226); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer fbCookieAsByteBuffer() { return __vector_as_bytebuffer(226, 1); }
  public String fbUserId() { int o = __offset(228); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer fbUserIdAsByteBuffer() { return __vector_as_bytebuffer(228, 1); }
  public String firstPurchaseStatEventId() { int o = __offset(230); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer firstPurchaseStatEventIdAsByteBuffer() { return __vector_as_bytebuffer(230, 1); }
  public String gclid() { int o = __offset(232); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer gclidAsByteBuffer() { return __vector_as_bytebuffer(232, 1); }
  public int gender() { int o = __offset(234); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public boolean googleAdTracking() { int o = __offset(236); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public String googleAid() { int o = __offset(238); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer googleAidAsByteBuffer() { return __vector_as_bytebuffer(238, 1); }
  public String googleUserId() { int o = __offset(240); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer googleUserIdAsByteBuffer() { return __vector_as_bytebuffer(240, 1); }
  public boolean iadAttribution() { int o = __offset(242); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public String id() { int o = __offset(244); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer idAsByteBuffer() { return __vector_as_bytebuffer(244, 1); }
  public String impressionCreated() { int o = __offset(246); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer impressionCreatedAsByteBuffer() { return __vector_as_bytebuffer(246, 1); }
  public int impressionGrossCount() { int o = __offset(248); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String impressionId() { int o = __offset(250); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer impressionIdAsByteBuffer() { return __vector_as_bytebuffer(250, 1); }
  public int impressionIdDate() { int o = __offset(252); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public long impressionIdUnique1() { int o = __offset(254); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public long impressionIdUnique2() { int o = __offset(256); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public String impressionMatchType() { int o = __offset(258); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer impressionMatchTypeAsByteBuffer() { return __vector_as_bytebuffer(258, 1); }
  public int impressionPublishersCount() { int o = __offset(260); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int impressionUniqueCount() { int o = __offset(262); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int impressions() { int o = __offset(264); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int installAdNetworkId() { int o = __offset(266); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String installAdvertiserSubAd() { int o = __offset(268); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installAdvertiserSubAdAsByteBuffer() { return __vector_as_bytebuffer(268, 1); }
  public String installAdvertiserSubAdgroup() { int o = __offset(270); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installAdvertiserSubAdgroupAsByteBuffer() { return __vector_as_bytebuffer(270, 1); }
  public String installAdvertiserSubCampaign() { int o = __offset(272); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installAdvertiserSubCampaignAsByteBuffer() { return __vector_as_bytebuffer(272, 1); }
  public String installAdvertiserSubKeyword() { int o = __offset(274); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installAdvertiserSubKeywordAsByteBuffer() { return __vector_as_bytebuffer(274, 1); }
  public String installAdvertiserSubPlacement() { int o = __offset(276); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installAdvertiserSubPlacementAsByteBuffer() { return __vector_as_bytebuffer(276, 1); }
  public String installAdvertiserSubPublisher() { int o = __offset(278); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installAdvertiserSubPublisherAsByteBuffer() { return __vector_as_bytebuffer(278, 1); }
  public String installAdvertiserSubSite() { int o = __offset(280); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installAdvertiserSubSiteAsByteBuffer() { return __vector_as_bytebuffer(280, 1); }
  public int installAgencyId() { int o = __offset(282); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String installAttributed() { int o = __offset(284); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installAttributedAsByteBuffer() { return __vector_as_bytebuffer(284, 1); }
  public String installCreated() { int o = __offset(286); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installCreatedAsByteBuffer() { return __vector_as_bytebuffer(286, 1); }
  public String installDate() { int o = __offset(288); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installDateAsByteBuffer() { return __vector_as_bytebuffer(288, 1); }
  public String installDateHour() { int o = __offset(290); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installDateHourAsByteBuffer() { return __vector_as_bytebuffer(290, 1); }
  public String installId() { int o = __offset(292); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installIdAsByteBuffer() { return __vector_as_bytebuffer(292, 1); }
  public int installIdDate() { int o = __offset(294); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public long installIdUnique1() { int o = __offset(296); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public long installIdUnique2() { int o = __offset(298); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public int installIntervalDay() { int o = __offset(300); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int installIntervalMonth() { int o = __offset(302); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int installIntervalWeek() { int o = __offset(304); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int installIntervalYear() { int o = __offset(306); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int installPublisherId() { int o = __offset(308); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String installPublisherSubAd() { int o = __offset(310); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installPublisherSubAdAsByteBuffer() { return __vector_as_bytebuffer(310, 1); }
  public String installPublisherSubAdgroup() { int o = __offset(312); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installPublisherSubAdgroupAsByteBuffer() { return __vector_as_bytebuffer(312, 1); }
  public String installPublisherSubCampaign() { int o = __offset(314); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installPublisherSubCampaignAsByteBuffer() { return __vector_as_bytebuffer(314, 1); }
  public String installPublisherSubKeyword() { int o = __offset(316); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installPublisherSubKeywordAsByteBuffer() { return __vector_as_bytebuffer(316, 1); }
  public String installPublisherSubPlacement() { int o = __offset(318); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installPublisherSubPlacementAsByteBuffer() { return __vector_as_bytebuffer(318, 1); }
  public String installPublisherSubPublisher() { int o = __offset(320); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installPublisherSubPublisherAsByteBuffer() { return __vector_as_bytebuffer(320, 1); }
  public String installPublisherSubSite() { int o = __offset(322); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installPublisherSubSiteAsByteBuffer() { return __vector_as_bytebuffer(322, 1); }
  public String installReferrer() { int o = __offset(324); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installReferrerAsByteBuffer() { return __vector_as_bytebuffer(324, 1); }
  public String installStatClickId() { int o = __offset(326); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installStatClickIdAsByteBuffer() { return __vector_as_bytebuffer(326, 1); }
  public String installStatImpressionId() { int o = __offset(328); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer installStatImpressionIdAsByteBuffer() { return __vector_as_bytebuffer(328, 1); }
  public boolean installViewThrough() { int o = __offset(330); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public int installs() { int o = __offset(332); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int iosAdTracking() { int o = __offset(334); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String iosIfa() { int o = __offset(336); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer iosIfaAsByteBuffer() { return __vector_as_bytebuffer(336, 1); }
  public String iosIfaMd5() { int o = __offset(338); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer iosIfaMd5AsByteBuffer() { return __vector_as_bytebuffer(338, 1); }
  public String iosIfaSha1() { int o = __offset(340); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer iosIfaSha1AsByteBuffer() { return __vector_as_bytebuffer(340, 1); }
  public String iosIfv() { int o = __offset(342); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer iosIfvAsByteBuffer() { return __vector_as_bytebuffer(342, 1); }
  public String iosIfvMd5() { int o = __offset(344); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer iosIfvMd5AsByteBuffer() { return __vector_as_bytebuffer(344, 1); }
  public String iosIfvSha1() { int o = __offset(346); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer iosIfvSha1AsByteBuffer() { return __vector_as_bytebuffer(346, 1); }
  public String ip() { int o = __offset(348); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer ipAsByteBuffer() { return __vector_as_bytebuffer(348, 1); }
  public boolean isAssist() { int o = __offset(350); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public boolean isCoppa() { int o = __offset(352); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public boolean isFirst() { int o = __offset(354); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public boolean isFirstPurchase() { int o = __offset(356); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public boolean isLast() { int o = __offset(358); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public boolean isNonwindowedAttribution() { int o = __offset(360); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public boolean isPaid() { int o = __offset(362); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public boolean isPurchase() { int o = __offset(364); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public boolean isReengagement() { int o = __offset(366); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public boolean isUnique() { int o = __offset(368); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public boolean isViewThrough() { int o = __offset(370); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public String language() { int o = __offset(372); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer languageAsByteBuffer() { return __vector_as_bytebuffer(372, 1); }
  public int languageId() { int o = __offset(374); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String lastOpenDatetime() { int o = __offset(376); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer lastOpenDatetimeAsByteBuffer() { return __vector_as_bytebuffer(376, 1); }
  public String lastReengagementPublisherId() { int o = __offset(378); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer lastReengagementPublisherIdAsByteBuffer() { return __vector_as_bytebuffer(378, 1); }
  public String lastReengagementStatEventId() { int o = __offset(380); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer lastReengagementStatEventIdAsByteBuffer() { return __vector_as_bytebuffer(380, 1); }
  public String lastReengagementStatOpenId() { int o = __offset(382); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer lastReengagementStatOpenIdAsByteBuffer() { return __vector_as_bytebuffer(382, 1); }
  public String lastReferralUrl() { int o = __offset(384); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer lastReferralUrlAsByteBuffer() { return __vector_as_bytebuffer(384, 1); }
  public String lastStatOpenId() { int o = __offset(386); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer lastStatOpenIdAsByteBuffer() { return __vector_as_bytebuffer(386, 1); }
  public double latitude() { int o = __offset(388); return o != 0 ? bb.getDouble(o + bb_pos) : 0; }
  public String level() { int o = __offset(390); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer levelAsByteBuffer() { return __vector_as_bytebuffer(390, 1); }
  public String logCreated() { int o = __offset(392); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer logCreatedAsByteBuffer() { return __vector_as_bytebuffer(392, 1); }
  public String logDateHour() { int o = __offset(394); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer logDateHourAsByteBuffer() { return __vector_as_bytebuffer(394, 1); }
  public String logId() { int o = __offset(396); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer logIdAsByteBuffer() { return __vector_as_bytebuffer(396, 1); }
  public int logIdDate() { int o = __offset(398); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public long logIdUnique1() { int o = __offset(400); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public long logIdUnique2() { int o = __offset(402); return o != 0 ? bb.getLong(o + bb_pos) : 0; }
  public String logStatus() { int o = __offset(404); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer logStatusAsByteBuffer() { return __vector_as_bytebuffer(404, 1); }
  public String logType() { int o = __offset(406); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer logTypeAsByteBuffer() { return __vector_as_bytebuffer(406, 1); }
  public double longitude() { int o = __offset(408); return o != 0 ? bb.getDouble(o + bb_pos) : 0; }
  public String macAddress() { int o = __offset(410); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer macAddressAsByteBuffer() { return __vector_as_bytebuffer(410, 1); }
  public String macAddressMd5() { int o = __offset(412); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer macAddressMd5AsByteBuffer() { return __vector_as_bytebuffer(412, 1); }
  public String macAddressSha1() { int o = __offset(414); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer macAddressSha1AsByteBuffer() { return __vector_as_bytebuffer(414, 1); }
  public String macAddressSha256() { int o = __offset(416); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer macAddressSha256AsByteBuffer() { return __vector_as_bytebuffer(416, 1); }
  public String matId() { int o = __offset(418); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer matIdAsByteBuffer() { return __vector_as_bytebuffer(418, 1); }
  public String matchType() { int o = __offset(420); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer matchTypeAsByteBuffer() { return __vector_as_bytebuffer(420, 1); }
  public String mobileAppType() { int o = __offset(422); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer mobileAppTypeAsByteBuffer() { return __vector_as_bytebuffer(422, 1); }
  public String modified() { int o = __offset(424); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer modifiedAsByteBuffer() { return __vector_as_bytebuffer(424, 1); }
  public String odin() { int o = __offset(426); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer odinAsByteBuffer() { return __vector_as_bytebuffer(426, 1); }
  public String openUdid() { int o = __offset(428); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer openUdidAsByteBuffer() { return __vector_as_bytebuffer(428, 1); }
  public int opens() { int o = __offset(430); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String osId() { int o = __offset(432); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer osIdAsByteBuffer() { return __vector_as_bytebuffer(432, 1); }
  public String osIdMd5() { int o = __offset(434); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer osIdMd5AsByteBuffer() { return __vector_as_bytebuffer(434, 1); }
  public String osIdSha1() { int o = __offset(436); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer osIdSha1AsByteBuffer() { return __vector_as_bytebuffer(436, 1); }
  public String osIdSha256() { int o = __offset(438); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer osIdSha256AsByteBuffer() { return __vector_as_bytebuffer(438, 1); }
  public boolean osJailbroke() { int o = __offset(440); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public String osVersion() { int o = __offset(442); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer osVersionAsByteBuffer() { return __vector_as_bytebuffer(442, 1); }
  public String packageName() { int o = __offset(444); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer packageNameAsByteBuffer() { return __vector_as_bytebuffer(444, 1); }
  public float payout() { int o = __offset(446); return o != 0 ? bb.getFloat(o + bb_pos) : 0; }
  public String payoutType() { int o = __offset(448); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer payoutTypeAsByteBuffer() { return __vector_as_bytebuffer(448, 1); }
  public int pi2() { int o = __offset(450); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int pi3() { int o = __offset(452); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int pi4() { int o = __offset(454); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int pi5() { int o = __offset(456); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String platform() { int o = __offset(458); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer platformAsByteBuffer() { return __vector_as_bytebuffer(458, 1); }
  public int publisherAdgroupId() { int o = __offset(460); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int publisherAdgroupItemId() { int o = __offset(462); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String publisherId() { int o = __offset(464); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherIdAsByteBuffer() { return __vector_as_bytebuffer(464, 1); }
  public int publisherLookbackHours() { int o = __offset(466); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String publisherRefId() { int o = __offset(468); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherRefIdAsByteBuffer() { return __vector_as_bytebuffer(468, 1); }
  public String publisherSubAd() { int o = __offset(470); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherSubAdAsByteBuffer() { return __vector_as_bytebuffer(470, 1); }
  public String publisherSubAdId() { int o = __offset(472); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherSubAdIdAsByteBuffer() { return __vector_as_bytebuffer(472, 1); }
  public String publisherSubAdgroup() { int o = __offset(474); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherSubAdgroupAsByteBuffer() { return __vector_as_bytebuffer(474, 1); }
  public String publisherSubAdgroupId() { int o = __offset(476); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherSubAdgroupIdAsByteBuffer() { return __vector_as_bytebuffer(476, 1); }
  public String publisherSubCampaign() { int o = __offset(478); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherSubCampaignAsByteBuffer() { return __vector_as_bytebuffer(478, 1); }
  public String publisherSubCampaignId() { int o = __offset(480); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherSubCampaignIdAsByteBuffer() { return __vector_as_bytebuffer(480, 1); }
  public String publisherSubKeyword() { int o = __offset(482); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherSubKeywordAsByteBuffer() { return __vector_as_bytebuffer(482, 1); }
  public String publisherSubKeywordId() { int o = __offset(484); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherSubKeywordIdAsByteBuffer() { return __vector_as_bytebuffer(484, 1); }
  public String publisherSubPlacement() { int o = __offset(486); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherSubPlacementAsByteBuffer() { return __vector_as_bytebuffer(486, 1); }
  public String publisherSubPlacementId() { int o = __offset(488); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherSubPlacementIdAsByteBuffer() { return __vector_as_bytebuffer(488, 1); }
  public String publisherSubPublisher() { int o = __offset(490); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherSubPublisherAsByteBuffer() { return __vector_as_bytebuffer(490, 1); }
  public String publisherSubPublisherId() { int o = __offset(492); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherSubPublisherIdAsByteBuffer() { return __vector_as_bytebuffer(492, 1); }
  public String publisherSubSite() { int o = __offset(494); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherSubSiteAsByteBuffer() { return __vector_as_bytebuffer(494, 1); }
  public String publisherSubSiteId() { int o = __offset(496); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherSubSiteIdAsByteBuffer() { return __vector_as_bytebuffer(496, 1); }
  public String publisherSub1() { int o = __offset(498); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherSub1AsByteBuffer() { return __vector_as_bytebuffer(498, 1); }
  public String publisherSub2() { int o = __offset(500); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherSub2AsByteBuffer() { return __vector_as_bytebuffer(500, 1); }
  public String publisherSub3() { int o = __offset(502); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherSub3AsByteBuffer() { return __vector_as_bytebuffer(502, 1); }
  public String publisherSub4() { int o = __offset(504); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherSub4AsByteBuffer() { return __vector_as_bytebuffer(504, 1); }
  public String publisherSub5() { int o = __offset(506); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherSub5AsByteBuffer() { return __vector_as_bytebuffer(506, 1); }
  public String publisherUserId() { int o = __offset(508); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer publisherUserIdAsByteBuffer() { return __vector_as_bytebuffer(508, 1); }
  public String purchaseStatus() { int o = __offset(510); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer purchaseStatusAsByteBuffer() { return __vector_as_bytebuffer(510, 1); }
  public boolean purchaseValidationStatus() { int o = __offset(512); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public String pv2() { int o = __offset(514); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer pv2AsByteBuffer() { return __vector_as_bytebuffer(514, 1); }
  public String pv3() { int o = __offset(516); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer pv3AsByteBuffer() { return __vector_as_bytebuffer(516, 1); }
  public String pv4() { int o = __offset(518); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer pv4AsByteBuffer() { return __vector_as_bytebuffer(518, 1); }
  public String pv5() { int o = __offset(520); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer pv5AsByteBuffer() { return __vector_as_bytebuffer(520, 1); }
  public int quantity() { int o = __offset(522); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public float rating() { int o = __offset(524); return o != 0 ? bb.getFloat(o + bb_pos) : 0; }
  public int reachTotal() { int o = __offset(526); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int reengagementAdNetworkId() { int o = __offset(528); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String reengagementAdvertiserSubAd() { int o = __offset(530); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementAdvertiserSubAdAsByteBuffer() { return __vector_as_bytebuffer(530, 1); }
  public String reengagementAdvertiserSubAdgroup() { int o = __offset(532); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementAdvertiserSubAdgroupAsByteBuffer() { return __vector_as_bytebuffer(532, 1); }
  public String reengagementAdvertiserSubCampaign() { int o = __offset(534); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementAdvertiserSubCampaignAsByteBuffer() { return __vector_as_bytebuffer(534, 1); }
  public String reengagementAdvertiserSubKeyword() { int o = __offset(536); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementAdvertiserSubKeywordAsByteBuffer() { return __vector_as_bytebuffer(536, 1); }
  public String reengagementAdvertiserSubPlacement() { int o = __offset(538); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementAdvertiserSubPlacementAsByteBuffer() { return __vector_as_bytebuffer(538, 1); }
  public String reengagementAdvertiserSubPublisher() { int o = __offset(540); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementAdvertiserSubPublisherAsByteBuffer() { return __vector_as_bytebuffer(540, 1); }
  public String reengagementAdvertiserSubSite() { int o = __offset(542); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementAdvertiserSubSiteAsByteBuffer() { return __vector_as_bytebuffer(542, 1); }
  public int reengagementAgencyId() { int o = __offset(544); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String reengagementAttributableCreated() { int o = __offset(546); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementAttributableCreatedAsByteBuffer() { return __vector_as_bytebuffer(546, 1); }
  public String reengagementAttributableDateHour() { int o = __offset(548); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementAttributableDateHourAsByteBuffer() { return __vector_as_bytebuffer(548, 1); }
  public String reengagementAttributableId() { int o = __offset(550); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementAttributableIdAsByteBuffer() { return __vector_as_bytebuffer(550, 1); }
  public int reengagementAttributableIdDate() { int o = __offset(552); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String reengagementAttributableIdUnique1() { int o = __offset(554); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementAttributableIdUnique1AsByteBuffer() { return __vector_as_bytebuffer(554, 1); }
  public String reengagementAttributableIdUnique2() { int o = __offset(556); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementAttributableIdUnique2AsByteBuffer() { return __vector_as_bytebuffer(556, 1); }
  public int reengagementAttributableIntervalDay() { int o = __offset(558); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int reengagementAttributableIntervalMonth() { int o = __offset(560); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int reengagementAttributableIntervalWeek() { int o = __offset(562); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String reengagementAttributableIntervalYear() { int o = __offset(564); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementAttributableIntervalYearAsByteBuffer() { return __vector_as_bytebuffer(564, 1); }
  public String reengagementAttributableType() { int o = __offset(566); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementAttributableTypeAsByteBuffer() { return __vector_as_bytebuffer(566, 1); }
  public String reengagementAttributed() { int o = __offset(568); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementAttributedAsByteBuffer() { return __vector_as_bytebuffer(568, 1); }
  public String reengagementCreated() { int o = __offset(570); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementCreatedAsByteBuffer() { return __vector_as_bytebuffer(570, 1); }
  public String reengagementDateHour() { int o = __offset(572); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementDateHourAsByteBuffer() { return __vector_as_bytebuffer(572, 1); }
  public String reengagementId() { int o = __offset(574); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementIdAsByteBuffer() { return __vector_as_bytebuffer(574, 1); }
  public int reengagementIdDate() { int o = __offset(576); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String reengagementIdUnique1() { int o = __offset(578); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementIdUnique1AsByteBuffer() { return __vector_as_bytebuffer(578, 1); }
  public String reengagementIdUnique2() { int o = __offset(580); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementIdUnique2AsByteBuffer() { return __vector_as_bytebuffer(580, 1); }
  public int reengagementIntervalDay() { int o = __offset(582); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int reengagementIntervalMonth() { int o = __offset(584); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int reengagementIntervalWeek() { int o = __offset(586); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String reengagementIntervalYear() { int o = __offset(588); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementIntervalYearAsByteBuffer() { return __vector_as_bytebuffer(588, 1); }
  public int reengagementPublisherId() { int o = __offset(590); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String reengagementPublisherSubAd() { int o = __offset(592); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementPublisherSubAdAsByteBuffer() { return __vector_as_bytebuffer(592, 1); }
  public String reengagementPublisherSubAdgroup() { int o = __offset(594); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementPublisherSubAdgroupAsByteBuffer() { return __vector_as_bytebuffer(594, 1); }
  public String reengagementPublisherSubCampaign() { int o = __offset(596); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementPublisherSubCampaignAsByteBuffer() { return __vector_as_bytebuffer(596, 1); }
  public String reengagementPublisherSubKeyword() { int o = __offset(598); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementPublisherSubKeywordAsByteBuffer() { return __vector_as_bytebuffer(598, 1); }
  public String reengagementPublisherSubPlacement() { int o = __offset(600); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementPublisherSubPlacementAsByteBuffer() { return __vector_as_bytebuffer(600, 1); }
  public String reengagementPublisherSubPublisher() { int o = __offset(602); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementPublisherSubPublisherAsByteBuffer() { return __vector_as_bytebuffer(602, 1); }
  public String reengagementPublisherSubSite() { int o = __offset(604); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementPublisherSubSiteAsByteBuffer() { return __vector_as_bytebuffer(604, 1); }
  public String reengagementType() { int o = __offset(606); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer reengagementTypeAsByteBuffer() { return __vector_as_bytebuffer(606, 1); }
  public boolean reengagementViewThrough() { int o = __offset(608); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public boolean reengagementviewThrough() { int o = __offset(610); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public String referralSource() { int o = __offset(612); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer referralSourceAsByteBuffer() { return __vector_as_bytebuffer(612, 1); }
  public String referralUrl() { int o = __offset(614); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer referralUrlAsByteBuffer() { return __vector_as_bytebuffer(614, 1); }
  public String regionCode() { int o = __offset(616); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer regionCodeAsByteBuffer() { return __vector_as_bytebuffer(616, 1); }
  public int regionId() { int o = __offset(618); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String requestUrl() { int o = __offset(620); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer requestUrlAsByteBuffer() { return __vector_as_bytebuffer(620, 1); }
  public float revenues() { int o = __offset(622); return o != 0 ? bb.getFloat(o + bb_pos) : 0; }
  public float revenuesUsd() { int o = __offset(624); return o != 0 ? bb.getFloat(o + bb_pos) : 0; }
  public float revenue() { int o = __offset(626); return o != 0 ? bb.getFloat(o + bb_pos) : 0; }
  public float revenueUsd() { int o = __offset(628); return o != 0 ? bb.getFloat(o + bb_pos) : 0; }
  public String sdk() { int o = __offset(630); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer sdkAsByteBuffer() { return __vector_as_bytebuffer(630, 1); }
  public String sdkPlugin() { int o = __offset(632); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer sdkPluginAsByteBuffer() { return __vector_as_bytebuffer(632, 1); }
  public String sdkVersion() { int o = __offset(634); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer sdkVersionAsByteBuffer() { return __vector_as_bytebuffer(634, 1); }
  public String searchString() { int o = __offset(636); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer searchStringAsByteBuffer() { return __vector_as_bytebuffer(636, 1); }
  public String sessionDatetime() { int o = __offset(638); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer sessionDatetimeAsByteBuffer() { return __vector_as_bytebuffer(638, 1); }
  public String sessionIp() { int o = __offset(640); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer sessionIpAsByteBuffer() { return __vector_as_bytebuffer(640, 1); }
  public int siteEventId() { int o = __offset(642); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int siteEventItemsCount() { int o = __offset(644); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int siteEventOffset() { int o = __offset(646); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String siteEventRef() { int o = __offset(648); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer siteEventRefAsByteBuffer() { return __vector_as_bytebuffer(648, 1); }
  public String siteEventType() { int o = __offset(650); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer siteEventTypeAsByteBuffer() { return __vector_as_bytebuffer(650, 1); }
  public int siteId() { int o = __offset(652); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int siteOffset() { int o = __offset(654); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String sourceFile() { int o = __offset(656); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer sourceFileAsByteBuffer() { return __vector_as_bytebuffer(656, 1); }
  public String statApprovedId() { int o = __offset(658); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer statApprovedIdAsByteBuffer() { return __vector_as_bytebuffer(658, 1); }
  public String statClickId() { int o = __offset(660); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer statClickIdAsByteBuffer() { return __vector_as_bytebuffer(660, 1); }
  public String statEventId() { int o = __offset(662); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer statEventIdAsByteBuffer() { return __vector_as_bytebuffer(662, 1); }
  public String statImpressionId() { int o = __offset(664); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer statImpressionIdAsByteBuffer() { return __vector_as_bytebuffer(664, 1); }
  public String statInstallId() { int o = __offset(666); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer statInstallIdAsByteBuffer() { return __vector_as_bytebuffer(666, 1); }
  public String statOpenId() { int o = __offset(668); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer statOpenIdAsByteBuffer() { return __vector_as_bytebuffer(668, 1); }
  public String statUpdateId() { int o = __offset(670); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer statUpdateIdAsByteBuffer() { return __vector_as_bytebuffer(670, 1); }
  public String status() { int o = __offset(672); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer statusAsByteBuffer() { return __vector_as_bytebuffer(672, 1); }
  public int statusCode() { int o = __offset(674); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String systemDate() { int o = __offset(676); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer systemDateAsByteBuffer() { return __vector_as_bytebuffer(676, 1); }
  public int testProfileId() { int o = __offset(678); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String tpid() { int o = __offset(680); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer tpidAsByteBuffer() { return __vector_as_bytebuffer(680, 1); }
  public String trackingId() { int o = __offset(682); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer trackingIdAsByteBuffer() { return __vector_as_bytebuffer(682, 1); }
  public String transactionId() { int o = __offset(684); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer transactionIdAsByteBuffer() { return __vector_as_bytebuffer(684, 1); }
  public int twitterAttribution() { int o = __offset(686); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String twitterEngagementDatetime() { int o = __offset(688); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer twitterEngagementDatetimeAsByteBuffer() { return __vector_as_bytebuffer(688, 1); }
  public String twitterNonwindowedAttribution() { int o = __offset(690); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer twitterNonwindowedAttributionAsByteBuffer() { return __vector_as_bytebuffer(690, 1); }
  public String twitterUserId() { int o = __offset(692); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer twitterUserIdAsByteBuffer() { return __vector_as_bytebuffer(692, 1); }
  public String twitterViewImpression() { int o = __offset(694); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer twitterViewImpressionAsByteBuffer() { return __vector_as_bytebuffer(694, 1); }
  public String unid() { int o = __offset(696); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer unidAsByteBuffer() { return __vector_as_bytebuffer(696, 1); }
  public float unverifiedRevenues() { int o = __offset(698); return o != 0 ? bb.getFloat(o + bb_pos) : 0; }
  public float unverifiedRevenuesUsd() { int o = __offset(700); return o != 0 ? bb.getFloat(o + bb_pos) : 0; }
  public int updates() { int o = __offset(702); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String userAgent() { int o = __offset(704); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer userAgentAsByteBuffer() { return __vector_as_bytebuffer(704, 1); }
  public String userEmail() { int o = __offset(706); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer userEmailAsByteBuffer() { return __vector_as_bytebuffer(706, 1); }
  public String userEmailMd5() { int o = __offset(708); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer userEmailMd5AsByteBuffer() { return __vector_as_bytebuffer(708, 1); }
  public String userEmailSha1() { int o = __offset(710); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer userEmailSha1AsByteBuffer() { return __vector_as_bytebuffer(710, 1); }
  public String userEmailSha256() { int o = __offset(712); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer userEmailSha256AsByteBuffer() { return __vector_as_bytebuffer(712, 1); }
  public String userId() { int o = __offset(714); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer userIdAsByteBuffer() { return __vector_as_bytebuffer(714, 1); }
  public String userIdMd5() { int o = __offset(716); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer userIdMd5AsByteBuffer() { return __vector_as_bytebuffer(716, 1); }
  public String userIdSha1() { int o = __offset(718); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer userIdSha1AsByteBuffer() { return __vector_as_bytebuffer(718, 1); }
  public String userPhone() { int o = __offset(720); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer userPhoneAsByteBuffer() { return __vector_as_bytebuffer(720, 1); }
  public String userPhoneMd5() { int o = __offset(722); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer userPhoneMd5AsByteBuffer() { return __vector_as_bytebuffer(722, 1); }
  public String userPhoneSha1() { int o = __offset(724); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer userPhoneSha1AsByteBuffer() { return __vector_as_bytebuffer(724, 1); }
  public String userPhoneSha256() { int o = __offset(726); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer userPhoneSha256AsByteBuffer() { return __vector_as_bytebuffer(726, 1); }
  public String username() { int o = __offset(728); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer usernameAsByteBuffer() { return __vector_as_bytebuffer(728, 1); }
  public String usernameMd5() { int o = __offset(730); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer usernameMd5AsByteBuffer() { return __vector_as_bytebuffer(730, 1); }
  public String usernameSha1() { int o = __offset(732); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer usernameSha1AsByteBuffer() { return __vector_as_bytebuffer(732, 1); }
  public String usernameSha256() { int o = __offset(734); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer usernameSha256AsByteBuffer() { return __vector_as_bytebuffer(734, 1); }
  public int viewMax() { int o = __offset(736); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public int viewPercent() { int o = __offset(738); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public boolean viewThrough() { int o = __offset(740); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }
  public String windowsAid() { int o = __offset(742); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer windowsAidAsByteBuffer() { return __vector_as_bytebuffer(742, 1); }
  public String windowsAidMd5() { int o = __offset(744); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer windowsAidMd5AsByteBuffer() { return __vector_as_bytebuffer(744, 1); }
  public String windowsAidSha1() { int o = __offset(746); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer windowsAidSha1AsByteBuffer() { return __vector_as_bytebuffer(746, 1); }
  public String wurflBrandName() { int o = __offset(748); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer wurflBrandNameAsByteBuffer() { return __vector_as_bytebuffer(748, 1); }
  public String wurflDeviceOs() { int o = __offset(750); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer wurflDeviceOsAsByteBuffer() { return __vector_as_bytebuffer(750, 1); }
  public String wurflDeviceOsVersion() { int o = __offset(752); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer wurflDeviceOsVersionAsByteBuffer() { return __vector_as_bytebuffer(752, 1); }
  public String wurflModelName() { int o = __offset(754); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer wurflModelNameAsByteBuffer() { return __vector_as_bytebuffer(754, 1); }

  

  public static void startrawLog(FlatBufferBuilder builder) { builder.startObject(376); }
  public static void addAdId(FlatBufferBuilder builder, int adIdOffset) { builder.addOffset(0, adIdOffset, 0); }
  public static void addAdNetworkId(FlatBufferBuilder builder, int adNetworkId) { builder.addInt(1, adNetworkId, 0); }
  public static void addAdvertiserFileId(FlatBufferBuilder builder, int advertiserFileId) { builder.addInt(2, advertiserFileId, 0); }
  public static void addAdvertiserId(FlatBufferBuilder builder, int advertiserId) { builder.addInt(3, advertiserId, 0); }
  public static void addAdvertiserRefId(FlatBufferBuilder builder, int advertiserRefIdOffset) { builder.addOffset(4, advertiserRefIdOffset, 0); }
  public static void addAdvertiserSubAd(FlatBufferBuilder builder, int advertiserSubAdOffset) { builder.addOffset(5, advertiserSubAdOffset, 0); }
  public static void addAdvertiserSubAdId(FlatBufferBuilder builder, int advertiserSubAdIdOffset) { builder.addOffset(6, advertiserSubAdIdOffset, 0); }
  public static void addAdvertiserSubAdgroup(FlatBufferBuilder builder, int advertiserSubAdgroupOffset) { builder.addOffset(7, advertiserSubAdgroupOffset, 0); }
  public static void addAdvertiserSubAdgroupId(FlatBufferBuilder builder, int advertiserSubAdgroupIdOffset) { builder.addOffset(8, advertiserSubAdgroupIdOffset, 0); }
  public static void addAdvertiserSubCampaign(FlatBufferBuilder builder, int advertiserSubCampaignOffset) { builder.addOffset(9, advertiserSubCampaignOffset, 0); }
  public static void addAdvertiserSubCampaignId(FlatBufferBuilder builder, int advertiserSubCampaignIdOffset) { builder.addOffset(10, advertiserSubCampaignIdOffset, 0); }
  public static void addAdvertiserSubKeyword(FlatBufferBuilder builder, int advertiserSubKeywordOffset) { builder.addOffset(11, advertiserSubKeywordOffset, 0); }
  public static void addAdvertiserSubKeywordId(FlatBufferBuilder builder, int advertiserSubKeywordIdOffset) { builder.addOffset(12, advertiserSubKeywordIdOffset, 0); }
  public static void addAdvertiserSubPlacement(FlatBufferBuilder builder, int advertiserSubPlacementOffset) { builder.addOffset(13, advertiserSubPlacementOffset, 0); }
  public static void addAdvertiserSubPlacementId(FlatBufferBuilder builder, int advertiserSubPlacementIdOffset) { builder.addOffset(14, advertiserSubPlacementIdOffset, 0); }
  public static void addAdvertiserSubPublisher(FlatBufferBuilder builder, int advertiserSubPublisherOffset) { builder.addOffset(15, advertiserSubPublisherOffset, 0); }
  public static void addAdvertiserSubPublisherId(FlatBufferBuilder builder, int advertiserSubPublisherIdOffset) { builder.addOffset(16, advertiserSubPublisherIdOffset, 0); }
  public static void addAdvertiserSubSite(FlatBufferBuilder builder, int advertiserSubSiteOffset) { builder.addOffset(17, advertiserSubSiteOffset, 0); }
  public static void addAdvertiserSubSiteId(FlatBufferBuilder builder, int advertiserSubSiteIdOffset) { builder.addOffset(18, advertiserSubSiteIdOffset, 0); }
  public static void addAge(FlatBufferBuilder builder, int age) { builder.addInt(19, age, 0); }
  public static void addAgencyId(FlatBufferBuilder builder, int agencyId) { builder.addInt(20, agencyId, 0); }
  public static void addAltitude(FlatBufferBuilder builder, double altitude) { builder.addDouble(21, altitude, 0); }
  public static void addAppName(FlatBufferBuilder builder, int appNameOffset) { builder.addOffset(22, appNameOffset, 0); }
  public static void addAppVersion(FlatBufferBuilder builder, int appVersionOffset) { builder.addOffset(23, appVersionOffset, 0); }
  public static void addAttributableCreated(FlatBufferBuilder builder, int attributableCreatedOffset) { builder.addOffset(24, attributableCreatedOffset, 0); }
  public static void addAttributableDateHour(FlatBufferBuilder builder, int attributableDateHourOffset) { builder.addOffset(25, attributableDateHourOffset, 0); }
  public static void addAttributableId(FlatBufferBuilder builder, int attributableIdOffset) { builder.addOffset(26, attributableIdOffset, 0); }
  public static void addAttributableIdDate(FlatBufferBuilder builder, int attributableIdDateOffset) { builder.addOffset(27, attributableIdDateOffset, 0); }
  public static void addAttributableIdUnique1(FlatBufferBuilder builder, long attributableIdUnique1) { builder.addLong(28, attributableIdUnique1, 0); }
  public static void addAttributableIdUnique2(FlatBufferBuilder builder, long attributableIdUnique2) { builder.addLong(29, attributableIdUnique2, 0); }
  public static void addAttributableIntervalDay(FlatBufferBuilder builder, int attributableIntervalDay) { builder.addInt(30, attributableIntervalDay, 0); }
  public static void addAttributableIntervalMonth(FlatBufferBuilder builder, int attributableIntervalMonth) { builder.addInt(31, attributableIntervalMonth, 0); }
  public static void addAttributableIntervalWeek(FlatBufferBuilder builder, int attributableIntervalWeek) { builder.addInt(32, attributableIntervalWeek, 0); }
  public static void addAttributableIntervalYear(FlatBufferBuilder builder, int attributableIntervalYearOffset) { builder.addOffset(33, attributableIntervalYearOffset, 0); }
  public static void addAttributableType(FlatBufferBuilder builder, int attributableTypeOffset) { builder.addOffset(34, attributableTypeOffset, 0); }
  public static void addAttributableViewThrough(FlatBufferBuilder builder, boolean attributableViewThrough) { builder.addBoolean(35, attributableViewThrough, false); }
  public static void addAttributeSub1(FlatBufferBuilder builder, int attributeSub1Offset) { builder.addOffset(36, attributeSub1Offset, 0); }
  public static void addAttributeSub2(FlatBufferBuilder builder, int attributeSub2Offset) { builder.addOffset(37, attributeSub2Offset, 0); }
  public static void addAttributeSub3(FlatBufferBuilder builder, int attributeSub3Offset) { builder.addOffset(38, attributeSub3Offset, 0); }
  public static void addAttributeSub4(FlatBufferBuilder builder, int attributeSub4Offset) { builder.addOffset(39, attributeSub4Offset, 0); }
  public static void addAttributeSub5(FlatBufferBuilder builder, int attributeSub5Offset) { builder.addOffset(40, attributeSub5Offset, 0); }
  public static void addAttributed(FlatBufferBuilder builder, int attributedOffset) { builder.addOffset(41, attributedOffset, 0); }
  public static void addAttributedAgencyId(FlatBufferBuilder builder, int attributedAgencyIdOffset) { builder.addOffset(42, attributedAgencyIdOffset, 0); }
  public static void addAttributedCreated(FlatBufferBuilder builder, int attributedCreatedOffset) { builder.addOffset(43, attributedCreatedOffset, 0); }
  public static void addAttributedId(FlatBufferBuilder builder, int attributedIdOffset) { builder.addOffset(44, attributedIdOffset, 0); }
  public static void addAttributedIdDate(FlatBufferBuilder builder, int attributedIdDate) { builder.addInt(45, attributedIdDate, 0); }
  public static void addAttributedIdUnique1(FlatBufferBuilder builder, long attributedIdUnique1) { builder.addLong(46, attributedIdUnique1, 0); }
  public static void addAttributedIdUnique2(FlatBufferBuilder builder, long attributedIdUnique2) { builder.addLong(47, attributedIdUnique2, 0); }
  public static void addAttributedPublisherId(FlatBufferBuilder builder, int attributedPublisherIdOffset) { builder.addOffset(48, attributedPublisherIdOffset, 0); }
  public static void addAttributedType(FlatBufferBuilder builder, int attributedTypeOffset) { builder.addOffset(49, attributedTypeOffset, 0); }
  public static void addAttributionMethod(FlatBufferBuilder builder, int attributionMethodOffset) { builder.addOffset(50, attributionMethodOffset, 0); }
  public static void addAttributionPercent(FlatBufferBuilder builder, float attributionPercent) { builder.addFloat(51, attributionPercent, 0); }
  public static void addAttributionPrecent(FlatBufferBuilder builder, float attributionPrecent) { builder.addFloat(52, attributionPrecent, 0); }
  public static void addCampaignAdId(FlatBufferBuilder builder, int campaignAdId) { builder.addInt(53, campaignAdId, 0); }
  public static void addCampaignId(FlatBufferBuilder builder, long campaignId) { builder.addLong(54, campaignId, 0); }
  public static void addCampaignPayoutId(FlatBufferBuilder builder, int campaignPayoutId) { builder.addInt(55, campaignPayoutId, 0); }
  public static void addCampaignUrlId(FlatBufferBuilder builder, int campaignUrlId) { builder.addInt(56, campaignUrlId, 0); }
  public static void addCandidateClickIds(FlatBufferBuilder builder, int candidateClickIdsOffset) { builder.addOffset(57, candidateClickIdsOffset, 0); }
  public static void addCandidateImpressionIds(FlatBufferBuilder builder, int candidateImpressionIdsOffset) { builder.addOffset(58, candidateImpressionIdsOffset, 0); }
  public static void addCarrierAid(FlatBufferBuilder builder, int carrierAidOffset) { builder.addOffset(59, carrierAidOffset, 0); }
  public static void addChannelId(FlatBufferBuilder builder, int channelId) { builder.addInt(60, channelId, 0); }
  public static void addClickCreated(FlatBufferBuilder builder, int clickCreatedOffset) { builder.addOffset(61, clickCreatedOffset, 0); }
  public static void addClickGrossCount(FlatBufferBuilder builder, int clickGrossCount) { builder.addInt(62, clickGrossCount, 0); }
  public static void addClickId(FlatBufferBuilder builder, int clickIdOffset) { builder.addOffset(63, clickIdOffset, 0); }
  public static void addClickIdDate(FlatBufferBuilder builder, int clickIdDate) { builder.addInt(64, clickIdDate, 0); }
  public static void addClickIdUnique1(FlatBufferBuilder builder, long clickIdUnique1) { builder.addLong(65, clickIdUnique1, 0); }
  public static void addClickIdUnique2(FlatBufferBuilder builder, long clickIdUnique2) { builder.addLong(66, clickIdUnique2, 0); }
  public static void addClickMatchType(FlatBufferBuilder builder, int clickMatchTypeOffset) { builder.addOffset(67, clickMatchTypeOffset, 0); }
  public static void addClickPublishersCount(FlatBufferBuilder builder, int clickPublishersCount) { builder.addInt(68, clickPublishersCount, 0); }
  public static void addClickUniqueCount(FlatBufferBuilder builder, int clickUniqueCount) { builder.addInt(69, clickUniqueCount, 0); }
  public static void addClickUrl(FlatBufferBuilder builder, int clickUrlOffset) { builder.addOffset(70, clickUrlOffset, 0); }
  public static void addClicks(FlatBufferBuilder builder, int clicks) { builder.addInt(71, clicks, 0); }
  public static void addConnectionSpeed(FlatBufferBuilder builder, int connectionSpeedOffset) { builder.addOffset(72, connectionSpeedOffset, 0); }
  public static void addContentId(FlatBufferBuilder builder, int contentIdOffset) { builder.addOffset(73, contentIdOffset, 0); }
  public static void addContentType(FlatBufferBuilder builder, int contentTypeOffset) { builder.addOffset(74, contentTypeOffset, 0); }
  public static void addConversionCreated(FlatBufferBuilder builder, int conversionCreatedOffset) { builder.addOffset(75, conversionCreatedOffset, 0); }
  public static void addConversionDateHour(FlatBufferBuilder builder, int conversionDateHourOffset) { builder.addOffset(76, conversionDateHourOffset, 0); }
  public static void addConversionId(FlatBufferBuilder builder, int conversionIdOffset) { builder.addOffset(77, conversionIdOffset, 0); }
  public static void addConversionIdDate(FlatBufferBuilder builder, int conversionIdDateOffset) { builder.addOffset(78, conversionIdDateOffset, 0); }
  public static void addConversionIdUnique1(FlatBufferBuilder builder, long conversionIdUnique1) { builder.addLong(79, conversionIdUnique1, 0); }
  public static void addConversionIdUnique2(FlatBufferBuilder builder, long conversionIdUnique2) { builder.addLong(80, conversionIdUnique2, 0); }
  public static void addConversionStatus(FlatBufferBuilder builder, int conversionStatusOffset) { builder.addOffset(81, conversionStatusOffset, 0); }
  public static void addConversionType(FlatBufferBuilder builder, int conversionTypeOffset) { builder.addOffset(82, conversionTypeOffset, 0); }
  public static void addCost(FlatBufferBuilder builder, float cost) { builder.addFloat(83, cost, 0); }
  public static void addCostModel(FlatBufferBuilder builder, int costModelOffset) { builder.addOffset(84, costModelOffset, 0); }
  public static void addCostTargetEventId(FlatBufferBuilder builder, int costTargetEventIdOffset) { builder.addOffset(85, costTargetEventIdOffset, 0); }
  public static void addCostTargetEventType(FlatBufferBuilder builder, int costTargetEventTypeOffset) { builder.addOffset(86, costTargetEventTypeOffset, 0); }
  public static void addCosts(FlatBufferBuilder builder, float costs) { builder.addFloat(87, costs, 0); }
  public static void addCountryCode(FlatBufferBuilder builder, int countryCodeOffset) { builder.addOffset(88, countryCodeOffset, 0); }
  public static void addCountryId(FlatBufferBuilder builder, int countryId) { builder.addInt(89, countryId, 0); }
  public static void addCreated(FlatBufferBuilder builder, int createdOffset) { builder.addOffset(90, createdOffset, 0); }
  public static void addCreatedDate(FlatBufferBuilder builder, int createdDateOffset) { builder.addOffset(91, createdDateOffset, 0); }
  public static void addCurrencyCode(FlatBufferBuilder builder, int currencyCodeOffset) { builder.addOffset(92, currencyCodeOffset, 0); }
  public static void addDate1(FlatBufferBuilder builder, int date1Offset) { builder.addOffset(93, date1Offset, 0); }
  public static void addDate2(FlatBufferBuilder builder, int date2Offset) { builder.addOffset(94, date2Offset, 0); }
  public static void addDebugMode(FlatBufferBuilder builder, boolean debugMode) { builder.addBoolean(95, debugMode, false); }
  public static void addDeviceBrand(FlatBufferBuilder builder, int deviceBrandOffset) { builder.addOffset(96, deviceBrandOffset, 0); }
  public static void addDeviceCarrier(FlatBufferBuilder builder, int deviceCarrierOffset) { builder.addOffset(97, deviceCarrierOffset, 0); }
  public static void addDeviceId(FlatBufferBuilder builder, int deviceIdOffset) { builder.addOffset(98, deviceIdOffset, 0); }
  public static void addDeviceIdMd5(FlatBufferBuilder builder, int deviceIdMd5Offset) { builder.addOffset(99, deviceIdMd5Offset, 0); }
  public static void addDeviceIdSha1(FlatBufferBuilder builder, int deviceIdSha1Offset) { builder.addOffset(100, deviceIdSha1Offset, 0); }
  public static void addDeviceIdSha256(FlatBufferBuilder builder, int deviceIdSha256Offset) { builder.addOffset(101, deviceIdSha256Offset, 0); }
  public static void addDeviceIp(FlatBufferBuilder builder, int deviceIpOffset) { builder.addOffset(102, deviceIpOffset, 0); }
  public static void addDeviceIpHash(FlatBufferBuilder builder, int deviceIpHashOffset) { builder.addOffset(103, deviceIpHashOffset, 0); }
  public static void addDeviceModel(FlatBufferBuilder builder, int deviceModelOffset) { builder.addOffset(104, deviceModelOffset, 0); }
  public static void addDeviceType(FlatBufferBuilder builder, int deviceTypeOffset) { builder.addOffset(105, deviceTypeOffset, 0); }
  public static void addEventItems(FlatBufferBuilder builder, int eventItemsOffset) { builder.addOffset(106, eventItemsOffset, 0); }
  public static void addEventType(FlatBufferBuilder builder, int eventTypeOffset) { builder.addOffset(107, eventTypeOffset, 0); }
  public static void addEvents(FlatBufferBuilder builder, int events) { builder.addInt(108, events, 0); }
  public static void addExistingUser(FlatBufferBuilder builder, boolean existingUser) { builder.addBoolean(109, existingUser, false); }
  public static void addFbAttribution(FlatBufferBuilder builder, boolean fbAttribution) { builder.addBoolean(110, fbAttribution, false); }
  public static void addFbCookie(FlatBufferBuilder builder, int fbCookieOffset) { builder.addOffset(111, fbCookieOffset, 0); }
  public static void addFbUserId(FlatBufferBuilder builder, int fbUserIdOffset) { builder.addOffset(112, fbUserIdOffset, 0); }
  public static void addFirstPurchaseStatEventId(FlatBufferBuilder builder, int firstPurchaseStatEventIdOffset) { builder.addOffset(113, firstPurchaseStatEventIdOffset, 0); }
  public static void addGclid(FlatBufferBuilder builder, int gclidOffset) { builder.addOffset(114, gclidOffset, 0); }
  public static void addGender(FlatBufferBuilder builder, int gender) { builder.addInt(115, gender, 0); }
  public static void addGoogleAdTracking(FlatBufferBuilder builder, boolean googleAdTracking) { builder.addBoolean(116, googleAdTracking, false); }
  public static void addGoogleAid(FlatBufferBuilder builder, int googleAidOffset) { builder.addOffset(117, googleAidOffset, 0); }
  public static void addGoogleUserId(FlatBufferBuilder builder, int googleUserIdOffset) { builder.addOffset(118, googleUserIdOffset, 0); }
  public static void addIadAttribution(FlatBufferBuilder builder, boolean iadAttribution) { builder.addBoolean(119, iadAttribution, false); }
  public static void addId(FlatBufferBuilder builder, int idOffset) { builder.addOffset(120, idOffset, 0); }
  public static void addImpressionCreated(FlatBufferBuilder builder, int impressionCreatedOffset) { builder.addOffset(121, impressionCreatedOffset, 0); }
  public static void addImpressionGrossCount(FlatBufferBuilder builder, int impressionGrossCount) { builder.addInt(122, impressionGrossCount, 0); }
  public static void addImpressionId(FlatBufferBuilder builder, int impressionIdOffset) { builder.addOffset(123, impressionIdOffset, 0); }
  public static void addImpressionIdDate(FlatBufferBuilder builder, int impressionIdDate) { builder.addInt(124, impressionIdDate, 0); }
  public static void addImpressionIdUnique1(FlatBufferBuilder builder, long impressionIdUnique1) { builder.addLong(125, impressionIdUnique1, 0); }
  public static void addImpressionIdUnique2(FlatBufferBuilder builder, long impressionIdUnique2) { builder.addLong(126, impressionIdUnique2, 0); }
  public static void addImpressionMatchType(FlatBufferBuilder builder, int impressionMatchTypeOffset) { builder.addOffset(127, impressionMatchTypeOffset, 0); }
  public static void addImpressionPublishersCount(FlatBufferBuilder builder, int impressionPublishersCount) { builder.addInt(128, impressionPublishersCount, 0); }
  public static void addImpressionUniqueCount(FlatBufferBuilder builder, int impressionUniqueCount) { builder.addInt(129, impressionUniqueCount, 0); }
  public static void addImpressions(FlatBufferBuilder builder, int impressions) { builder.addInt(130, impressions, 0); }
  public static void addInstallAdNetworkId(FlatBufferBuilder builder, int installAdNetworkId) { builder.addInt(131, installAdNetworkId, 0); }
  public static void addInstallAdvertiserSubAd(FlatBufferBuilder builder, int installAdvertiserSubAdOffset) { builder.addOffset(132, installAdvertiserSubAdOffset, 0); }
  public static void addInstallAdvertiserSubAdgroup(FlatBufferBuilder builder, int installAdvertiserSubAdgroupOffset) { builder.addOffset(133, installAdvertiserSubAdgroupOffset, 0); }
  public static void addInstallAdvertiserSubCampaign(FlatBufferBuilder builder, int installAdvertiserSubCampaignOffset) { builder.addOffset(134, installAdvertiserSubCampaignOffset, 0); }
  public static void addInstallAdvertiserSubKeyword(FlatBufferBuilder builder, int installAdvertiserSubKeywordOffset) { builder.addOffset(135, installAdvertiserSubKeywordOffset, 0); }
  public static void addInstallAdvertiserSubPlacement(FlatBufferBuilder builder, int installAdvertiserSubPlacementOffset) { builder.addOffset(136, installAdvertiserSubPlacementOffset, 0); }
  public static void addInstallAdvertiserSubPublisher(FlatBufferBuilder builder, int installAdvertiserSubPublisherOffset) { builder.addOffset(137, installAdvertiserSubPublisherOffset, 0); }
  public static void addInstallAdvertiserSubSite(FlatBufferBuilder builder, int installAdvertiserSubSiteOffset) { builder.addOffset(138, installAdvertiserSubSiteOffset, 0); }
  public static void addInstallAgencyId(FlatBufferBuilder builder, int installAgencyId) { builder.addInt(139, installAgencyId, 0); }
  public static void addInstallAttributed(FlatBufferBuilder builder, int installAttributedOffset) { builder.addOffset(140, installAttributedOffset, 0); }
  public static void addInstallCreated(FlatBufferBuilder builder, int installCreatedOffset) { builder.addOffset(141, installCreatedOffset, 0); }
  public static void addInstallDate(FlatBufferBuilder builder, int installDateOffset) { builder.addOffset(142, installDateOffset, 0); }
  public static void addInstallDateHour(FlatBufferBuilder builder, int installDateHourOffset) { builder.addOffset(143, installDateHourOffset, 0); }
  public static void addInstallId(FlatBufferBuilder builder, int installIdOffset) { builder.addOffset(144, installIdOffset, 0); }
  public static void addInstallIdDate(FlatBufferBuilder builder, int installIdDate) { builder.addInt(145, installIdDate, 0); }
  public static void addInstallIdUnique1(FlatBufferBuilder builder, long installIdUnique1) { builder.addLong(146, installIdUnique1, 0); }
  public static void addInstallIdUnique2(FlatBufferBuilder builder, long installIdUnique2) { builder.addLong(147, installIdUnique2, 0); }
  public static void addInstallIntervalDay(FlatBufferBuilder builder, int installIntervalDay) { builder.addInt(148, installIntervalDay, 0); }
  public static void addInstallIntervalMonth(FlatBufferBuilder builder, int installIntervalMonth) { builder.addInt(149, installIntervalMonth, 0); }
  public static void addInstallIntervalWeek(FlatBufferBuilder builder, int installIntervalWeek) { builder.addInt(150, installIntervalWeek, 0); }
  public static void addInstallIntervalYear(FlatBufferBuilder builder, int installIntervalYear) { builder.addInt(151, installIntervalYear, 0); }
  public static void addInstallPublisherId(FlatBufferBuilder builder, int installPublisherId) { builder.addInt(152, installPublisherId, 0); }
  public static void addInstallPublisherSubAd(FlatBufferBuilder builder, int installPublisherSubAdOffset) { builder.addOffset(153, installPublisherSubAdOffset, 0); }
  public static void addInstallPublisherSubAdgroup(FlatBufferBuilder builder, int installPublisherSubAdgroupOffset) { builder.addOffset(154, installPublisherSubAdgroupOffset, 0); }
  public static void addInstallPublisherSubCampaign(FlatBufferBuilder builder, int installPublisherSubCampaignOffset) { builder.addOffset(155, installPublisherSubCampaignOffset, 0); }
  public static void addInstallPublisherSubKeyword(FlatBufferBuilder builder, int installPublisherSubKeywordOffset) { builder.addOffset(156, installPublisherSubKeywordOffset, 0); }
  public static void addInstallPublisherSubPlacement(FlatBufferBuilder builder, int installPublisherSubPlacementOffset) { builder.addOffset(157, installPublisherSubPlacementOffset, 0); }
  public static void addInstallPublisherSubPublisher(FlatBufferBuilder builder, int installPublisherSubPublisherOffset) { builder.addOffset(158, installPublisherSubPublisherOffset, 0); }
  public static void addInstallPublisherSubSite(FlatBufferBuilder builder, int installPublisherSubSiteOffset) { builder.addOffset(159, installPublisherSubSiteOffset, 0); }
  public static void addInstallReferrer(FlatBufferBuilder builder, int installReferrerOffset) { builder.addOffset(160, installReferrerOffset, 0); }
  public static void addInstallStatClickId(FlatBufferBuilder builder, int installStatClickIdOffset) { builder.addOffset(161, installStatClickIdOffset, 0); }
  public static void addInstallStatImpressionId(FlatBufferBuilder builder, int installStatImpressionIdOffset) { builder.addOffset(162, installStatImpressionIdOffset, 0); }
  public static void addInstallViewThrough(FlatBufferBuilder builder, boolean installViewThrough) { builder.addBoolean(163, installViewThrough, false); }
  public static void addInstalls(FlatBufferBuilder builder, int installs) { builder.addInt(164, installs, 0); }
  public static void addIosAdTracking(FlatBufferBuilder builder, int iosAdTracking) { builder.addInt(165, iosAdTracking, 0); }
  public static void addIosIfa(FlatBufferBuilder builder, int iosIfaOffset) { builder.addOffset(166, iosIfaOffset, 0); }
  public static void addIosIfaMd5(FlatBufferBuilder builder, int iosIfaMd5Offset) { builder.addOffset(167, iosIfaMd5Offset, 0); }
  public static void addIosIfaSha1(FlatBufferBuilder builder, int iosIfaSha1Offset) { builder.addOffset(168, iosIfaSha1Offset, 0); }
  public static void addIosIfv(FlatBufferBuilder builder, int iosIfvOffset) { builder.addOffset(169, iosIfvOffset, 0); }
  public static void addIosIfvMd5(FlatBufferBuilder builder, int iosIfvMd5Offset) { builder.addOffset(170, iosIfvMd5Offset, 0); }
  public static void addIosIfvSha1(FlatBufferBuilder builder, int iosIfvSha1Offset) { builder.addOffset(171, iosIfvSha1Offset, 0); }
  public static void addIp(FlatBufferBuilder builder, int ipOffset) { builder.addOffset(172, ipOffset, 0); }
  public static void addIsAssist(FlatBufferBuilder builder, boolean isAssist) { builder.addBoolean(173, isAssist, false); }
  public static void addIsCoppa(FlatBufferBuilder builder, boolean isCoppa) { builder.addBoolean(174, isCoppa, false); }
  public static void addIsFirst(FlatBufferBuilder builder, boolean isFirst) { builder.addBoolean(175, isFirst, false); }
  public static void addIsFirstPurchase(FlatBufferBuilder builder, boolean isFirstPurchase) { builder.addBoolean(176, isFirstPurchase, false); }
  public static void addIsLast(FlatBufferBuilder builder, boolean isLast) { builder.addBoolean(177, isLast, false); }
  public static void addIsNonwindowedAttribution(FlatBufferBuilder builder, boolean isNonwindowedAttribution) { builder.addBoolean(178, isNonwindowedAttribution, false); }
  public static void addIsPaid(FlatBufferBuilder builder, boolean isPaid) { builder.addBoolean(179, isPaid, false); }
  public static void addIsPurchase(FlatBufferBuilder builder, boolean isPurchase) { builder.addBoolean(180, isPurchase, false); }
  public static void addIsReengagement(FlatBufferBuilder builder, boolean isReengagement) { builder.addBoolean(181, isReengagement, false); }
  public static void addIsUnique(FlatBufferBuilder builder, boolean isUnique) { builder.addBoolean(182, isUnique, false); }
  public static void addIsViewThrough(FlatBufferBuilder builder, boolean isViewThrough) { builder.addBoolean(183, isViewThrough, false); }
  public static void addLanguage(FlatBufferBuilder builder, int languageOffset) { builder.addOffset(184, languageOffset, 0); }
  public static void addLanguageId(FlatBufferBuilder builder, int languageId) { builder.addInt(185, languageId, 0); }
  public static void addLastOpenDatetime(FlatBufferBuilder builder, int lastOpenDatetimeOffset) { builder.addOffset(186, lastOpenDatetimeOffset, 0); }
  public static void addLastReengagementPublisherId(FlatBufferBuilder builder, int lastReengagementPublisherIdOffset) { builder.addOffset(187, lastReengagementPublisherIdOffset, 0); }
  public static void addLastReengagementStatEventId(FlatBufferBuilder builder, int lastReengagementStatEventIdOffset) { builder.addOffset(188, lastReengagementStatEventIdOffset, 0); }
  public static void addLastReengagementStatOpenId(FlatBufferBuilder builder, int lastReengagementStatOpenIdOffset) { builder.addOffset(189, lastReengagementStatOpenIdOffset, 0); }
  public static void addLastReferralUrl(FlatBufferBuilder builder, int lastReferralUrlOffset) { builder.addOffset(190, lastReferralUrlOffset, 0); }
  public static void addLastStatOpenId(FlatBufferBuilder builder, int lastStatOpenIdOffset) { builder.addOffset(191, lastStatOpenIdOffset, 0); }
  public static void addLatitude(FlatBufferBuilder builder, double latitude) { builder.addDouble(192, latitude, 0); }
  public static void addLevel(FlatBufferBuilder builder, int levelOffset) { builder.addOffset(193, levelOffset, 0); }
  public static void addLogCreated(FlatBufferBuilder builder, int logCreatedOffset) { builder.addOffset(194, logCreatedOffset, 0); }
  public static void addLogDateHour(FlatBufferBuilder builder, int logDateHourOffset) { builder.addOffset(195, logDateHourOffset, 0); }
  public static void addLogId(FlatBufferBuilder builder, int logIdOffset) { builder.addOffset(196, logIdOffset, 0); }
  public static void addLogIdDate(FlatBufferBuilder builder, int logIdDate) { builder.addInt(197, logIdDate, 0); }
  public static void addLogIdUnique1(FlatBufferBuilder builder, long logIdUnique1) { builder.addLong(198, logIdUnique1, 0); }
  public static void addLogIdUnique2(FlatBufferBuilder builder, long logIdUnique2) { builder.addLong(199, logIdUnique2, 0); }
  public static void addLogStatus(FlatBufferBuilder builder, int logStatusOffset) { builder.addOffset(200, logStatusOffset, 0); }
  public static void addLogType(FlatBufferBuilder builder, int logTypeOffset) { builder.addOffset(201, logTypeOffset, 0); }
  public static void addLongitude(FlatBufferBuilder builder, double longitude) { builder.addDouble(202, longitude, 0); }
  public static void addMacAddress(FlatBufferBuilder builder, int macAddressOffset) { builder.addOffset(203, macAddressOffset, 0); }
  public static void addMacAddressMd5(FlatBufferBuilder builder, int macAddressMd5Offset) { builder.addOffset(204, macAddressMd5Offset, 0); }
  public static void addMacAddressSha1(FlatBufferBuilder builder, int macAddressSha1Offset) { builder.addOffset(205, macAddressSha1Offset, 0); }
  public static void addMacAddressSha256(FlatBufferBuilder builder, int macAddressSha256Offset) { builder.addOffset(206, macAddressSha256Offset, 0); }
  public static void addMatId(FlatBufferBuilder builder, int matIdOffset) { builder.addOffset(207, matIdOffset, 0); }
  public static void addMatchType(FlatBufferBuilder builder, int matchTypeOffset) { builder.addOffset(208, matchTypeOffset, 0); }
  public static void addMobileAppType(FlatBufferBuilder builder, int mobileAppTypeOffset) { builder.addOffset(209, mobileAppTypeOffset, 0); }
  public static void addModified(FlatBufferBuilder builder, int modifiedOffset) { builder.addOffset(210, modifiedOffset, 0); }
  public static void addOdin(FlatBufferBuilder builder, int odinOffset) { builder.addOffset(211, odinOffset, 0); }
  public static void addOpenUdid(FlatBufferBuilder builder, int openUdidOffset) { builder.addOffset(212, openUdidOffset, 0); }
  public static void addOpens(FlatBufferBuilder builder, int opens) { builder.addInt(213, opens, 0); }
  public static void addOsId(FlatBufferBuilder builder, int osIdOffset) { builder.addOffset(214, osIdOffset, 0); }
  public static void addOsIdMd5(FlatBufferBuilder builder, int osIdMd5Offset) { builder.addOffset(215, osIdMd5Offset, 0); }
  public static void addOsIdSha1(FlatBufferBuilder builder, int osIdSha1Offset) { builder.addOffset(216, osIdSha1Offset, 0); }
  public static void addOsIdSha256(FlatBufferBuilder builder, int osIdSha256Offset) { builder.addOffset(217, osIdSha256Offset, 0); }
  public static void addOsJailbroke(FlatBufferBuilder builder, boolean osJailbroke) { builder.addBoolean(218, osJailbroke, false); }
  public static void addOsVersion(FlatBufferBuilder builder, int osVersionOffset) { builder.addOffset(219, osVersionOffset, 0); }
  public static void addPackageName(FlatBufferBuilder builder, int packageNameOffset) { builder.addOffset(220, packageNameOffset, 0); }
  public static void addPayout(FlatBufferBuilder builder, float payout) { builder.addFloat(221, payout, 0); }
  public static void addPayoutType(FlatBufferBuilder builder, int payoutTypeOffset) { builder.addOffset(222, payoutTypeOffset, 0); }
  public static void addPi2(FlatBufferBuilder builder, int pi2) { builder.addInt(223, pi2, 0); }
  public static void addPi3(FlatBufferBuilder builder, int pi3) { builder.addInt(224, pi3, 0); }
  public static void addPi4(FlatBufferBuilder builder, int pi4) { builder.addInt(225, pi4, 0); }
  public static void addPi5(FlatBufferBuilder builder, int pi5) { builder.addInt(226, pi5, 0); }
  public static void addPlatform(FlatBufferBuilder builder, int platformOffset) { builder.addOffset(227, platformOffset, 0); }
  public static void addPublisherAdgroupId(FlatBufferBuilder builder, int publisherAdgroupId) { builder.addInt(228, publisherAdgroupId, 0); }
  public static void addPublisherAdgroupItemId(FlatBufferBuilder builder, int publisherAdgroupItemId) { builder.addInt(229, publisherAdgroupItemId, 0); }
  public static void addPublisherId(FlatBufferBuilder builder, int publisherIdOffset) { builder.addOffset(230, publisherIdOffset, 0); }
  public static void addPublisherLookbackHours(FlatBufferBuilder builder, int publisherLookbackHours) { builder.addInt(231, publisherLookbackHours, 0); }
  public static void addPublisherRefId(FlatBufferBuilder builder, int publisherRefIdOffset) { builder.addOffset(232, publisherRefIdOffset, 0); }
  public static void addPublisherSubAd(FlatBufferBuilder builder, int publisherSubAdOffset) { builder.addOffset(233, publisherSubAdOffset, 0); }
  public static void addPublisherSubAdId(FlatBufferBuilder builder, int publisherSubAdIdOffset) { builder.addOffset(234, publisherSubAdIdOffset, 0); }
  public static void addPublisherSubAdgroup(FlatBufferBuilder builder, int publisherSubAdgroupOffset) { builder.addOffset(235, publisherSubAdgroupOffset, 0); }
  public static void addPublisherSubAdgroupId(FlatBufferBuilder builder, int publisherSubAdgroupIdOffset) { builder.addOffset(236, publisherSubAdgroupIdOffset, 0); }
  public static void addPublisherSubCampaign(FlatBufferBuilder builder, int publisherSubCampaignOffset) { builder.addOffset(237, publisherSubCampaignOffset, 0); }
  public static void addPublisherSubCampaignId(FlatBufferBuilder builder, int publisherSubCampaignIdOffset) { builder.addOffset(238, publisherSubCampaignIdOffset, 0); }
  public static void addPublisherSubKeyword(FlatBufferBuilder builder, int publisherSubKeywordOffset) { builder.addOffset(239, publisherSubKeywordOffset, 0); }
  public static void addPublisherSubKeywordId(FlatBufferBuilder builder, int publisherSubKeywordIdOffset) { builder.addOffset(240, publisherSubKeywordIdOffset, 0); }
  public static void addPublisherSubPlacement(FlatBufferBuilder builder, int publisherSubPlacementOffset) { builder.addOffset(241, publisherSubPlacementOffset, 0); }
  public static void addPublisherSubPlacementId(FlatBufferBuilder builder, int publisherSubPlacementIdOffset) { builder.addOffset(242, publisherSubPlacementIdOffset, 0); }
  public static void addPublisherSubPublisher(FlatBufferBuilder builder, int publisherSubPublisherOffset) { builder.addOffset(243, publisherSubPublisherOffset, 0); }
  public static void addPublisherSubPublisherId(FlatBufferBuilder builder, int publisherSubPublisherIdOffset) { builder.addOffset(244, publisherSubPublisherIdOffset, 0); }
  public static void addPublisherSubSite(FlatBufferBuilder builder, int publisherSubSiteOffset) { builder.addOffset(245, publisherSubSiteOffset, 0); }
  public static void addPublisherSubSiteId(FlatBufferBuilder builder, int publisherSubSiteIdOffset) { builder.addOffset(246, publisherSubSiteIdOffset, 0); }
  public static void addPublisherSub1(FlatBufferBuilder builder, int publisherSub1Offset) { builder.addOffset(247, publisherSub1Offset, 0); }
  public static void addPublisherSub2(FlatBufferBuilder builder, int publisherSub2Offset) { builder.addOffset(248, publisherSub2Offset, 0); }
  public static void addPublisherSub3(FlatBufferBuilder builder, int publisherSub3Offset) { builder.addOffset(249, publisherSub3Offset, 0); }
  public static void addPublisherSub4(FlatBufferBuilder builder, int publisherSub4Offset) { builder.addOffset(250, publisherSub4Offset, 0); }
  public static void addPublisherSub5(FlatBufferBuilder builder, int publisherSub5Offset) { builder.addOffset(251, publisherSub5Offset, 0); }
  public static void addPublisherUserId(FlatBufferBuilder builder, int publisherUserIdOffset) { builder.addOffset(252, publisherUserIdOffset, 0); }
  public static void addPurchaseStatus(FlatBufferBuilder builder, int purchaseStatusOffset) { builder.addOffset(253, purchaseStatusOffset, 0); }
  public static void addPurchaseValidationStatus(FlatBufferBuilder builder, boolean purchaseValidationStatus) { builder.addBoolean(254, purchaseValidationStatus, false); }
  public static void addPv2(FlatBufferBuilder builder, int pv2Offset) { builder.addOffset(255, pv2Offset, 0); }
  public static void addPv3(FlatBufferBuilder builder, int pv3Offset) { builder.addOffset(256, pv3Offset, 0); }
  public static void addPv4(FlatBufferBuilder builder, int pv4Offset) { builder.addOffset(257, pv4Offset, 0); }
  public static void addPv5(FlatBufferBuilder builder, int pv5Offset) { builder.addOffset(258, pv5Offset, 0); }
  public static void addQuantity(FlatBufferBuilder builder, int quantity) { builder.addInt(259, quantity, 0); }
  public static void addRating(FlatBufferBuilder builder, float rating) { builder.addFloat(260, rating, 0); }
  public static void addReachTotal(FlatBufferBuilder builder, int reachTotal) { builder.addInt(261, reachTotal, 0); }
  public static void addReengagementAdNetworkId(FlatBufferBuilder builder, int reengagementAdNetworkId) { builder.addInt(262, reengagementAdNetworkId, 0); }
  public static void addReengagementAdvertiserSubAd(FlatBufferBuilder builder, int reengagementAdvertiserSubAdOffset) { builder.addOffset(263, reengagementAdvertiserSubAdOffset, 0); }
  public static void addReengagementAdvertiserSubAdgroup(FlatBufferBuilder builder, int reengagementAdvertiserSubAdgroupOffset) { builder.addOffset(264, reengagementAdvertiserSubAdgroupOffset, 0); }
  public static void addReengagementAdvertiserSubCampaign(FlatBufferBuilder builder, int reengagementAdvertiserSubCampaignOffset) { builder.addOffset(265, reengagementAdvertiserSubCampaignOffset, 0); }
  public static void addReengagementAdvertiserSubKeyword(FlatBufferBuilder builder, int reengagementAdvertiserSubKeywordOffset) { builder.addOffset(266, reengagementAdvertiserSubKeywordOffset, 0); }
  public static void addReengagementAdvertiserSubPlacement(FlatBufferBuilder builder, int reengagementAdvertiserSubPlacementOffset) { builder.addOffset(267, reengagementAdvertiserSubPlacementOffset, 0); }
  public static void addReengagementAdvertiserSubPublisher(FlatBufferBuilder builder, int reengagementAdvertiserSubPublisherOffset) { builder.addOffset(268, reengagementAdvertiserSubPublisherOffset, 0); }
  public static void addReengagementAdvertiserSubSite(FlatBufferBuilder builder, int reengagementAdvertiserSubSiteOffset) { builder.addOffset(269, reengagementAdvertiserSubSiteOffset, 0); }
  public static void addReengagementAgencyId(FlatBufferBuilder builder, int reengagementAgencyId) { builder.addInt(270, reengagementAgencyId, 0); }
  public static void addReengagementAttributableCreated(FlatBufferBuilder builder, int reengagementAttributableCreatedOffset) { builder.addOffset(271, reengagementAttributableCreatedOffset, 0); }
  public static void addReengagementAttributableDateHour(FlatBufferBuilder builder, int reengagementAttributableDateHourOffset) { builder.addOffset(272, reengagementAttributableDateHourOffset, 0); }
  public static void addReengagementAttributableId(FlatBufferBuilder builder, int reengagementAttributableIdOffset) { builder.addOffset(273, reengagementAttributableIdOffset, 0); }
  public static void addReengagementAttributableIdDate(FlatBufferBuilder builder, int reengagementAttributableIdDate) { builder.addInt(274, reengagementAttributableIdDate, 0); }
  public static void addReengagementAttributableIdUnique1(FlatBufferBuilder builder, int reengagementAttributableIdUnique1Offset) { builder.addOffset(275, reengagementAttributableIdUnique1Offset, 0); }
  public static void addReengagementAttributableIdUnique2(FlatBufferBuilder builder, int reengagementAttributableIdUnique2Offset) { builder.addOffset(276, reengagementAttributableIdUnique2Offset, 0); }
  public static void addReengagementAttributableIntervalDay(FlatBufferBuilder builder, int reengagementAttributableIntervalDay) { builder.addInt(277, reengagementAttributableIntervalDay, 0); }
  public static void addReengagementAttributableIntervalMonth(FlatBufferBuilder builder, int reengagementAttributableIntervalMonth) { builder.addInt(278, reengagementAttributableIntervalMonth, 0); }
  public static void addReengagementAttributableIntervalWeek(FlatBufferBuilder builder, int reengagementAttributableIntervalWeek) { builder.addInt(279, reengagementAttributableIntervalWeek, 0); }
  public static void addReengagementAttributableIntervalYear(FlatBufferBuilder builder, int reengagementAttributableIntervalYearOffset) { builder.addOffset(280, reengagementAttributableIntervalYearOffset, 0); }
  public static void addReengagementAttributableType(FlatBufferBuilder builder, int reengagementAttributableTypeOffset) { builder.addOffset(281, reengagementAttributableTypeOffset, 0); }
  public static void addReengagementAttributed(FlatBufferBuilder builder, int reengagementAttributedOffset) { builder.addOffset(282, reengagementAttributedOffset, 0); }
  public static void addReengagementCreated(FlatBufferBuilder builder, int reengagementCreatedOffset) { builder.addOffset(283, reengagementCreatedOffset, 0); }
  public static void addReengagementDateHour(FlatBufferBuilder builder, int reengagementDateHourOffset) { builder.addOffset(284, reengagementDateHourOffset, 0); }
  public static void addReengagementId(FlatBufferBuilder builder, int reengagementIdOffset) { builder.addOffset(285, reengagementIdOffset, 0); }
  public static void addReengagementIdDate(FlatBufferBuilder builder, int reengagementIdDate) { builder.addInt(286, reengagementIdDate, 0); }
  public static void addReengagementIdUnique1(FlatBufferBuilder builder, int reengagementIdUnique1Offset) { builder.addOffset(287, reengagementIdUnique1Offset, 0); }
  public static void addReengagementIdUnique2(FlatBufferBuilder builder, int reengagementIdUnique2Offset) { builder.addOffset(288, reengagementIdUnique2Offset, 0); }
  public static void addReengagementIntervalDay(FlatBufferBuilder builder, int reengagementIntervalDay) { builder.addInt(289, reengagementIntervalDay, 0); }
  public static void addReengagementIntervalMonth(FlatBufferBuilder builder, int reengagementIntervalMonth) { builder.addInt(290, reengagementIntervalMonth, 0); }
  public static void addReengagementIntervalWeek(FlatBufferBuilder builder, int reengagementIntervalWeek) { builder.addInt(291, reengagementIntervalWeek, 0); }
  public static void addReengagementIntervalYear(FlatBufferBuilder builder, int reengagementIntervalYearOffset) { builder.addOffset(292, reengagementIntervalYearOffset, 0); }
  public static void addReengagementPublisherId(FlatBufferBuilder builder, int reengagementPublisherId) { builder.addInt(293, reengagementPublisherId, 0); }
  public static void addReengagementPublisherSubAd(FlatBufferBuilder builder, int reengagementPublisherSubAdOffset) { builder.addOffset(294, reengagementPublisherSubAdOffset, 0); }
  public static void addReengagementPublisherSubAdgroup(FlatBufferBuilder builder, int reengagementPublisherSubAdgroupOffset) { builder.addOffset(295, reengagementPublisherSubAdgroupOffset, 0); }
  public static void addReengagementPublisherSubCampaign(FlatBufferBuilder builder, int reengagementPublisherSubCampaignOffset) { builder.addOffset(296, reengagementPublisherSubCampaignOffset, 0); }
  public static void addReengagementPublisherSubKeyword(FlatBufferBuilder builder, int reengagementPublisherSubKeywordOffset) { builder.addOffset(297, reengagementPublisherSubKeywordOffset, 0); }
  public static void addReengagementPublisherSubPlacement(FlatBufferBuilder builder, int reengagementPublisherSubPlacementOffset) { builder.addOffset(298, reengagementPublisherSubPlacementOffset, 0); }
  public static void addReengagementPublisherSubPublisher(FlatBufferBuilder builder, int reengagementPublisherSubPublisherOffset) { builder.addOffset(299, reengagementPublisherSubPublisherOffset, 0); }
  public static void addReengagementPublisherSubSite(FlatBufferBuilder builder, int reengagementPublisherSubSiteOffset) { builder.addOffset(300, reengagementPublisherSubSiteOffset, 0); }
  public static void addReengagementType(FlatBufferBuilder builder, int reengagementTypeOffset) { builder.addOffset(301, reengagementTypeOffset, 0); }
  public static void addReengagementViewThrough(FlatBufferBuilder builder, boolean reengagementViewThrough) { builder.addBoolean(302, reengagementViewThrough, false); }
  public static void addReengagementviewThrough(FlatBufferBuilder builder, boolean reengagementviewThrough) { builder.addBoolean(303, reengagementviewThrough, false); }
  public static void addReferralSource(FlatBufferBuilder builder, int referralSourceOffset) { builder.addOffset(304, referralSourceOffset, 0); }
  public static void addReferralUrl(FlatBufferBuilder builder, int referralUrlOffset) { builder.addOffset(305, referralUrlOffset, 0); }
  public static void addRegionCode(FlatBufferBuilder builder, int regionCodeOffset) { builder.addOffset(306, regionCodeOffset, 0); }
  public static void addRegionId(FlatBufferBuilder builder, int regionId) { builder.addInt(307, regionId, 0); }
  public static void addRequestUrl(FlatBufferBuilder builder, int requestUrlOffset) { builder.addOffset(308, requestUrlOffset, 0); }
  public static void addRevenues(FlatBufferBuilder builder, float revenues) { builder.addFloat(309, revenues, 0); }
  public static void addRevenuesUsd(FlatBufferBuilder builder, float revenuesUsd) { builder.addFloat(310, revenuesUsd, 0); }
  public static void addRevenue(FlatBufferBuilder builder, float revenue) { builder.addFloat(311, revenue, 0); }
  public static void addRevenueUsd(FlatBufferBuilder builder, float revenueUsd) { builder.addFloat(312, revenueUsd, 0); }
  public static void addSdk(FlatBufferBuilder builder, int sdkOffset) { builder.addOffset(313, sdkOffset, 0); }
  public static void addSdkPlugin(FlatBufferBuilder builder, int sdkPluginOffset) { builder.addOffset(314, sdkPluginOffset, 0); }
  public static void addSdkVersion(FlatBufferBuilder builder, int sdkVersionOffset) { builder.addOffset(315, sdkVersionOffset, 0); }
  public static void addSearchString(FlatBufferBuilder builder, int searchStringOffset) { builder.addOffset(316, searchStringOffset, 0); }
  public static void addSessionDatetime(FlatBufferBuilder builder, int sessionDatetimeOffset) { builder.addOffset(317, sessionDatetimeOffset, 0); }
  public static void addSessionIp(FlatBufferBuilder builder, int sessionIpOffset) { builder.addOffset(318, sessionIpOffset, 0); }
  public static void addSiteEventId(FlatBufferBuilder builder, int siteEventId) { builder.addInt(319, siteEventId, 0); }
  public static void addSiteEventItemsCount(FlatBufferBuilder builder, int siteEventItemsCount) { builder.addInt(320, siteEventItemsCount, 0); }
  public static void addSiteEventOffset(FlatBufferBuilder builder, int siteEventOffset) { builder.addInt(321, siteEventOffset, 0); }
  public static void addSiteEventRef(FlatBufferBuilder builder, int siteEventRefOffset) { builder.addOffset(322, siteEventRefOffset, 0); }
  public static void addSiteEventType(FlatBufferBuilder builder, int siteEventTypeOffset) { builder.addOffset(323, siteEventTypeOffset, 0); }
  public static void addSiteId(FlatBufferBuilder builder, int siteId) { builder.addInt(324, siteId, 0); }
  public static void addSiteOffset(FlatBufferBuilder builder, int siteOffset) { builder.addInt(325, siteOffset, 0); }
  public static void addSourceFile(FlatBufferBuilder builder, int sourceFileOffset) { builder.addOffset(326, sourceFileOffset, 0); }
  public static void addStatApprovedId(FlatBufferBuilder builder, int statApprovedIdOffset) { builder.addOffset(327, statApprovedIdOffset, 0); }
  public static void addStatClickId(FlatBufferBuilder builder, int statClickIdOffset) { builder.addOffset(328, statClickIdOffset, 0); }
  public static void addStatEventId(FlatBufferBuilder builder, int statEventIdOffset) { builder.addOffset(329, statEventIdOffset, 0); }
  public static void addStatImpressionId(FlatBufferBuilder builder, int statImpressionIdOffset) { builder.addOffset(330, statImpressionIdOffset, 0); }
  public static void addStatInstallId(FlatBufferBuilder builder, int statInstallIdOffset) { builder.addOffset(331, statInstallIdOffset, 0); }
  public static void addStatOpenId(FlatBufferBuilder builder, int statOpenIdOffset) { builder.addOffset(332, statOpenIdOffset, 0); }
  public static void addStatUpdateId(FlatBufferBuilder builder, int statUpdateIdOffset) { builder.addOffset(333, statUpdateIdOffset, 0); }
  public static void addStatus(FlatBufferBuilder builder, int statusOffset) { builder.addOffset(334, statusOffset, 0); }
  public static void addStatusCode(FlatBufferBuilder builder, int statusCode) { builder.addInt(335, statusCode, 0); }
  public static void addSystemDate(FlatBufferBuilder builder, int systemDateOffset) { builder.addOffset(336, systemDateOffset, 0); }
  public static void addTestProfileId(FlatBufferBuilder builder, int testProfileId) { builder.addInt(337, testProfileId, 0); }
  public static void addTpid(FlatBufferBuilder builder, int tpidOffset) { builder.addOffset(338, tpidOffset, 0); }
  public static void addTrackingId(FlatBufferBuilder builder, int trackingIdOffset) { builder.addOffset(339, trackingIdOffset, 0); }
  public static void addTransactionId(FlatBufferBuilder builder, int transactionIdOffset) { builder.addOffset(340, transactionIdOffset, 0); }
  public static void addTwitterAttribution(FlatBufferBuilder builder, int twitterAttribution) { builder.addInt(341, twitterAttribution, 0); }
  public static void addTwitterEngagementDatetime(FlatBufferBuilder builder, int twitterEngagementDatetimeOffset) { builder.addOffset(342, twitterEngagementDatetimeOffset, 0); }
  public static void addTwitterNonwindowedAttribution(FlatBufferBuilder builder, int twitterNonwindowedAttributionOffset) { builder.addOffset(343, twitterNonwindowedAttributionOffset, 0); }
  public static void addTwitterUserId(FlatBufferBuilder builder, int twitterUserIdOffset) { builder.addOffset(344, twitterUserIdOffset, 0); }
  public static void addTwitterViewImpression(FlatBufferBuilder builder, int twitterViewImpressionOffset) { builder.addOffset(345, twitterViewImpressionOffset, 0); }
  public static void addUnid(FlatBufferBuilder builder, int unidOffset) { builder.addOffset(346, unidOffset, 0); }
  public static void addUnverifiedRevenues(FlatBufferBuilder builder, float unverifiedRevenues) { builder.addFloat(347, unverifiedRevenues, 0); }
  public static void addUnverifiedRevenuesUsd(FlatBufferBuilder builder, float unverifiedRevenuesUsd) { builder.addFloat(348, unverifiedRevenuesUsd, 0); }
  public static void addUpdates(FlatBufferBuilder builder, int updates) { builder.addInt(349, updates, 0); }
  public static void addUserAgent(FlatBufferBuilder builder, int userAgentOffset) { builder.addOffset(350, userAgentOffset, 0); }
  public static void addUserEmail(FlatBufferBuilder builder, int userEmailOffset) { builder.addOffset(351, userEmailOffset, 0); }
  public static void addUserEmailMd5(FlatBufferBuilder builder, int userEmailMd5Offset) { builder.addOffset(352, userEmailMd5Offset, 0); }
  public static void addUserEmailSha1(FlatBufferBuilder builder, int userEmailSha1Offset) { builder.addOffset(353, userEmailSha1Offset, 0); }
  public static void addUserEmailSha256(FlatBufferBuilder builder, int userEmailSha256Offset) { builder.addOffset(354, userEmailSha256Offset, 0); }
  public static void addUserId(FlatBufferBuilder builder, int userIdOffset) { builder.addOffset(355, userIdOffset, 0); }
  public static void addUserIdMd5(FlatBufferBuilder builder, int userIdMd5Offset) { builder.addOffset(356, userIdMd5Offset, 0); }
  public static void addUserIdSha1(FlatBufferBuilder builder, int userIdSha1Offset) { builder.addOffset(357, userIdSha1Offset, 0); }
  public static void addUserPhone(FlatBufferBuilder builder, int userPhoneOffset) { builder.addOffset(358, userPhoneOffset, 0); }
  public static void addUserPhoneMd5(FlatBufferBuilder builder, int userPhoneMd5Offset) { builder.addOffset(359, userPhoneMd5Offset, 0); }
  public static void addUserPhoneSha1(FlatBufferBuilder builder, int userPhoneSha1Offset) { builder.addOffset(360, userPhoneSha1Offset, 0); }
  public static void addUserPhoneSha256(FlatBufferBuilder builder, int userPhoneSha256Offset) { builder.addOffset(361, userPhoneSha256Offset, 0); }
  public static void addUsername(FlatBufferBuilder builder, int usernameOffset) { builder.addOffset(362, usernameOffset, 0); }
  public static void addUsernameMd5(FlatBufferBuilder builder, int usernameMd5Offset) { builder.addOffset(363, usernameMd5Offset, 0); }
  public static void addUsernameSha1(FlatBufferBuilder builder, int usernameSha1Offset) { builder.addOffset(364, usernameSha1Offset, 0); }
  public static void addUsernameSha256(FlatBufferBuilder builder, int usernameSha256Offset) { builder.addOffset(365, usernameSha256Offset, 0); }
  public static void addViewMax(FlatBufferBuilder builder, int viewMax) { builder.addInt(366, viewMax, 0); }
  public static void addViewPercent(FlatBufferBuilder builder, int viewPercent) { builder.addInt(367, viewPercent, 0); }
  public static void addViewThrough(FlatBufferBuilder builder, boolean viewThrough) { builder.addBoolean(368, viewThrough, false); }
  public static void addWindowsAid(FlatBufferBuilder builder, int windowsAidOffset) { builder.addOffset(369, windowsAidOffset, 0); }
  public static void addWindowsAidMd5(FlatBufferBuilder builder, int windowsAidMd5Offset) { builder.addOffset(370, windowsAidMd5Offset, 0); }
  public static void addWindowsAidSha1(FlatBufferBuilder builder, int windowsAidSha1Offset) { builder.addOffset(371, windowsAidSha1Offset, 0); }
  public static void addWurflBrandName(FlatBufferBuilder builder, int wurflBrandNameOffset) { builder.addOffset(372, wurflBrandNameOffset, 0); }
  public static void addWurflDeviceOs(FlatBufferBuilder builder, int wurflDeviceOsOffset) { builder.addOffset(373, wurflDeviceOsOffset, 0); }
  public static void addWurflDeviceOsVersion(FlatBufferBuilder builder, int wurflDeviceOsVersionOffset) { builder.addOffset(374, wurflDeviceOsVersionOffset, 0); }
  public static void addWurflModelName(FlatBufferBuilder builder, int wurflModelNameOffset) { builder.addOffset(375, wurflModelNameOffset, 0); }
  public static int endrawLog(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishrawLogBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset, "DATS"); }
};


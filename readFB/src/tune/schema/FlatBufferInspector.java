package tune.schema;



import google.flatbuffers.Constants;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;
/*
import com.google.common.base.CaseFormat;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;*/
import tune.schema.rawLog;

/**
 * This is manual FlatBuffer file inspector.
 * It simply prints contents of FlatBuffer file
 *
 */
public class FlatBufferInspector {
    public static HashMap<String, Method> getterMap;   
    public Object o;
    private final Logger logger = Logger.getLogger(getClass());

    public FlatBufferInspector() {
        getterMap = new HashMap<String,Method>();
    }

    public static void main(String[] args) {
        
        FlatBufferInspector fbInspector = new FlatBufferInspector();
        
        File file = new File("/home/fuat/Documentation/flatBuffer/rawLog");
        File fbsFile = new File("/home/fuat/2015/03/21/shard14_20150321_rev0_prison00_batcher00_seq0.fb.gz");
        //fbInspector.createGetterCache(file);
        //fbInspector.inspectSequential(fbsFile);

        fbInspector.inspectRandom(fbsFile, 4, 15038);   // block index, record index

  
    }
    
    /**
     * This method take uncompressed FlatBuffers file as input and print contents of block number and index number from the block
     * 
     * @param file  uncompressed FlatBuffers binary file
     * @param idxBock   block index starting from 0
     * @param idxMessage    offset index of block starting from 0
     */
    private void inspectRandom(File file, int idxBock, int idxMessage){ // these index starts from 0
        try {
            RandomAccessFile randFile = new RandomAccessFile(file, "r");
            long position = Constants.BLOCK_SIZE * (idxBock);
            randFile.seek(position);

            byte[] block = new byte[Constants.BLOCK_SIZE];

            byte[] data = null;
            byte[] bSize = new byte[Constants.SIZE_FIELD_SIZE];
            
            randFile.read(block);
            randFile.close();
            
            int offset = 0;
            int num = 0;
            ByteBuffer wrapped = null;
            for(int i=0; i < idxMessage; i++){
                bSize = Arrays.copyOfRange(block, offset, offset+ Constants.SIZE_FIELD_SIZE);
                wrapped = ByteBuffer.wrap(bSize); 
                num = wrapped.getInt();
                offset += Constants.SIZE_FIELD_SIZE;
                offset += num;
            }
            bSize = Arrays.copyOfRange(block, offset, offset + Constants.SIZE_FIELD_SIZE);
            wrapped = ByteBuffer.wrap(bSize); 
            num = wrapped.getInt();
            offset += Constants.SIZE_FIELD_SIZE;
            data = Arrays.copyOfRange(block, offset, (offset+ num) );
            printData(data);
            
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }

    /**
     * This method take gzipped Flatbuffer file as input and print all the records in the file
     * 
     * @param file  Gzipped FlatBuffers binary file
     */
  /*  private void inspectSequential(File file) {
        byte[] data = null;
        byte[] bSize = new byte[Constants.SIZE_FIELD_SIZE];
        int total = 0;
        
        try {
            data = ByteStreams.toByteArray(new GZIPInputStream(new FileInputStream(file)));
        } catch(java.io.IOException e) {
                logger.error("Cannot read flatbuffer file");
                return;
        }

        ByteBuffer bb = ByteBuffer.wrap(data);
        bb.rewind();
        int cumSize = 0;
        while (bb.hasRemaining()) {
                if (cumSize == Constants.BLOCK_SIZE) cumSize=0;
                int size = bb.getInt();
                cumSize+=size+Constants.SIZE_FIELD_SIZE;
                if (size==0 ) {
                        int paddingSize = Constants.BLOCK_SIZE-cumSize;
                        logger.debug("padding size is "+paddingSize);
                        if (paddingSize>0) {
                                byte[] padding = new byte[paddingSize];
                                bb.get(padding);
                        }
                        cumSize = 0;
                        continue;
                }
    
                byte[] record = new byte[size];
                bb.get(record);
              
                printData(record);
        }

    }
*/
   
    private void printData2(byte[] data) {
        // print small sampling of field
        ByteBuffer bb = ByteBuffer.wrap(data);
        rawLog rawLog =   tune.schema.rawLog.getRootAsrawLog(bb);
        System.out.println("adId(): "  + rawLog.adId());
        System.out.println("wurflDeviceOs(): "  + rawLog.wurflDeviceOs());
        System.out.println("wurflDeviceOsVersion(): "  + rawLog.wurflDeviceOsVersion());
        System.out.println("wurflModelName(): "  + rawLog.wurflModelName());

    }
    
    /**
     * This method just simply print all fields of Flatbuffers object
     * @param data
     */
    private void printData(byte[] data) {
        System.out.println("=============== Flatbuffer contents ===============");
        ByteBuffer bb = ByteBuffer.wrap(data);
        
        rawLog rawLog1 =   tune.schema.rawLog.getRootAsrawLog(bb);
        System.out.println(rawLog1.advertiserId());
        
        // If reflection not working use this code
       /* System.out.println("adId(): "  + rawLog.adId());
        System.out.println("adNetworkId(): "  + rawLog.adNetworkId());
        System.out.println("advertiserFileId(): "  + rawLog.advertiserFileId());
        System.out.println("advertiserId(): "  + rawLog.advertiserId());
        System.out.println("advertiserRefId(): "  + rawLog.advertiserRefId());
        System.out.println("advertiserSubAd(): "  + rawLog.advertiserSubAd());
        System.out.println("advertiserSubAdId(): "  + rawLog.advertiserSubAdId());
        System.out.println("advertiserSubAdgroup(): "  + rawLog.advertiserSubAdgroup());
        System.out.println("advertiserSubAdgroupId(): "  + rawLog.advertiserSubAdgroupId());
        System.out.println("advertiserSubCampaign(): "  + rawLog.advertiserSubCampaign());
        System.out.println("advertiserSubCampaignId(): "  + rawLog.advertiserSubCampaignId());
        System.out.println("advertiserSubKeyword(): "  + rawLog.advertiserSubKeyword());
        System.out.println("advertiserSubKeywordId(): "  + rawLog.advertiserSubKeywordId());
        System.out.println("advertiserSubPlacement(): "  + rawLog.advertiserSubPlacement());
        System.out.println("advertiserSubPlacementId(): "  + rawLog.advertiserSubPlacementId());
        System.out.println("advertiserSubPublisher(): "  + rawLog.advertiserSubPublisher());
        System.out.println("advertiserSubPublisherId(): "  + rawLog.advertiserSubPublisherId());
        System.out.println("advertiserSubSite(): "  + rawLog.advertiserSubSite());
        System.out.println("advertiserSubSiteId(): "  + rawLog.advertiserSubSiteId());
        System.out.println("age(): "  + rawLog.age());
        System.out.println("agencyId(): "  + rawLog.agencyId());
        System.out.println("altitude(): "  + rawLog.altitude());
        System.out.println("appName(): "  + rawLog.appName());
        System.out.println("appVersion(): "  + rawLog.appVersion());
        System.out.println("attributableCreated(): "  + rawLog.attributableCreated());
        System.out.println("attributableDateHour(): "  + rawLog.attributableDateHour());
        System.out.println("attributableId(): "  + rawLog.attributableId());
        System.out.println("attributableIdDate(): "  + rawLog.attributableIdDate());
        System.out.println("attributableIdUnique1(): "  + rawLog.attributableIdUnique1());
        System.out.println("attributableIdUnique2(): "  + rawLog.attributableIdUnique2());
        System.out.println("attributableIntervalDay(): "  + rawLog.attributableIntervalDay());
        System.out.println("attributableIntervalMonth(): "  + rawLog.attributableIntervalMonth());
        System.out.println("attributableIntervalWeek(): "  + rawLog.attributableIntervalWeek());
        System.out.println("attributableIntervalYear(): "  + rawLog.attributableIntervalYear());
        System.out.println("attributableType(): "  + rawLog.attributableType());
        System.out.println("attributableViewThrough(): "  + rawLog.attributableViewThrough());
        System.out.println("attributeSub1(): "  + rawLog.attributeSub1());
        System.out.println("attributeSub2(): "  + rawLog.attributeSub2());
        System.out.println("attributeSub3(): "  + rawLog.attributeSub3());
        System.out.println("attributeSub4(): "  + rawLog.attributeSub4());
        System.out.println("attributeSub5(): "  + rawLog.attributeSub5());
        System.out.println("attributed(): "  + rawLog.attributed());
        System.out.println("attributedAgencyId(): "  + rawLog.attributedAgencyId());
        System.out.println("attributedCreated(): "  + rawLog.attributedCreated());
        System.out.println("attributedId(): "  + rawLog.attributedId());
        System.out.println("attributedIdDate(): "  + rawLog.attributedIdDate());
        System.out.println("attributedIdUnique1(): "  + rawLog.attributedIdUnique1());
        System.out.println("attributedIdUnique2(): "  + rawLog.attributedIdUnique2());
        System.out.println("attributedPublisherId(): "  + rawLog.attributedPublisherId());
        System.out.println("attributedType(): "  + rawLog.attributedType());
        System.out.println("attributionMethod(): "  + rawLog.attributionMethod());
        System.out.println("attributionPercent(): "  + rawLog.attributionPercent());
        System.out.println("attributionPrecent(): "  + rawLog.attributionPrecent());
        System.out.println("campaignAdId(): "  + rawLog.campaignAdId());
        System.out.println("campaignId(): "  + rawLog.campaignId());
        System.out.println("campaignPayoutId(): "  + rawLog.campaignPayoutId());
        System.out.println("campaignUrlId(): "  + rawLog.campaignUrlId());
        System.out.println("candidateClickIds(): "  + rawLog.candidateClickIds());
        System.out.println("candidateImpressionIds(): "  + rawLog.candidateImpressionIds());
        System.out.println("carrierAid(): "  + rawLog.carrierAid());
        System.out.println("channelId(): "  + rawLog.channelId());
        System.out.println("clickCreated(): "  + rawLog.clickCreated());
        System.out.println("clickGrossCount(): "  + rawLog.clickGrossCount());
        System.out.println("clickId(): "  + rawLog.clickId());
        System.out.println("clickIdDate(): "  + rawLog.clickIdDate());
        System.out.println("clickIdUnique1(): "  + rawLog.clickIdUnique1());
        System.out.println("clickIdUnique2(): "  + rawLog.clickIdUnique2());
        System.out.println("clickMatchType(): "  + rawLog.clickMatchType());
        System.out.println("clickPublishersCount(): "  + rawLog.clickPublishersCount());
        System.out.println("clickUniqueCount(): "  + rawLog.clickUniqueCount());
        System.out.println("clickUrl(): "  + rawLog.clickUrl());
        System.out.println("clicks(): "  + rawLog.clicks());
        System.out.println("connectionSpeed(): "  + rawLog.connectionSpeed());
        System.out.println("contentId(): "  + rawLog.contentId());
        System.out.println("contentType(): "  + rawLog.contentType());
        System.out.println("conversionCreated(): "  + rawLog.conversionCreated());
        System.out.println("conversionDateHour(): "  + rawLog.conversionDateHour());
        System.out.println("conversionId(): "  + rawLog.conversionId());
        System.out.println("conversionIdDate(): "  + rawLog.conversionIdDate());
        System.out.println("conversionIdUnique1(): "  + rawLog.conversionIdUnique1());
        System.out.println("conversionIdUnique2(): "  + rawLog.conversionIdUnique2());
        System.out.println("conversionStatus(): "  + rawLog.conversionStatus());
        System.out.println("conversionType(): "  + rawLog.conversionType());
        System.out.println("cost(): "  + rawLog.cost());
        System.out.println("costModel(): "  + rawLog.costModel());
        System.out.println("costTargetEventId(): "  + rawLog.costTargetEventId());
        System.out.println("costTargetEventType(): "  + rawLog.costTargetEventType());
        System.out.println("costs(): "  + rawLog.costs());
        System.out.println("countryCode(): "  + rawLog.countryCode());
        System.out.println("countryId(): "  + rawLog.countryId());
        System.out.println("created(): "  + rawLog.created());
        System.out.println("createdDate(): "  + rawLog.createdDate());
        System.out.println("currencyCode(): "  + rawLog.currencyCode());
        System.out.println("date1(): "  + rawLog.date1());
        System.out.println("date2(): "  + rawLog.date2());
        System.out.println("debugMode(): "  + rawLog.debugMode());
        System.out.println("deviceBrand(): "  + rawLog.deviceBrand());
        System.out.println("deviceCarrier(): "  + rawLog.deviceCarrier());
        System.out.println("deviceId(): "  + rawLog.deviceId());
        System.out.println("deviceIdMd5(): "  + rawLog.deviceIdMd5());
        System.out.println("deviceIdSha1(): "  + rawLog.deviceIdSha1());
        System.out.println("deviceIdSha256(): "  + rawLog.deviceIdSha256());
        System.out.println("deviceIp(): "  + rawLog.deviceIp());
        System.out.println("deviceIpHash(): "  + rawLog.deviceIpHash());
        System.out.println("deviceModel(): "  + rawLog.deviceModel());
        System.out.println("deviceType(): "  + rawLog.deviceType());
        System.out.println("eventItems(): "  + rawLog.eventItems());
        System.out.println("eventType(): "  + rawLog.eventType());
        System.out.println("events(): "  + rawLog.events());
        System.out.println("existingUser(): "  + rawLog.existingUser());
        System.out.println("fbAttribution(): "  + rawLog.fbAttribution());
        System.out.println("fbCookie(): "  + rawLog.fbCookie());
        System.out.println("fbUserId(): "  + rawLog.fbUserId());
        System.out.println("firstPurchaseStatEventId(): "  + rawLog.firstPurchaseStatEventId());
        System.out.println("gclid(): "  + rawLog.gclid());
        System.out.println("gender(): "  + rawLog.gender());
        System.out.println("googleAdTracking(): "  + rawLog.googleAdTracking());
        System.out.println("googleAid(): "  + rawLog.googleAid());
        System.out.println("googleUserId(): "  + rawLog.googleUserId());
        System.out.println("iadAttribution(): "  + rawLog.iadAttribution());
        System.out.println("id(): "  + rawLog.id());
        System.out.println("impressionCreated(): "  + rawLog.impressionCreated());
        System.out.println("impressionGrossCount(): "  + rawLog.impressionGrossCount());
        System.out.println("impressionId(): "  + rawLog.impressionId());
        System.out.println("impressionIdDate(): "  + rawLog.impressionIdDate());
        System.out.println("impressionIdUnique1(): "  + rawLog.impressionIdUnique1());
        System.out.println("impressionIdUnique2(): "  + rawLog.impressionIdUnique2());
        System.out.println("impressionMatchType(): "  + rawLog.impressionMatchType());
        System.out.println("impressionPublishersCount(): "  + rawLog.impressionPublishersCount());
        System.out.println("impressionUniqueCount(): "  + rawLog.impressionUniqueCount());
        System.out.println("impressions(): "  + rawLog.impressions());
        System.out.println("installAdNetworkId(): "  + rawLog.installAdNetworkId());
        System.out.println("installAdvertiserSubAd(): "  + rawLog.installAdvertiserSubAd());
        System.out.println("installAdvertiserSubAdgroup(): "  + rawLog.installAdvertiserSubAdgroup());
        System.out.println("installAdvertiserSubCampaign(): "  + rawLog.installAdvertiserSubCampaign());
        System.out.println("installAdvertiserSubKeyword(): "  + rawLog.installAdvertiserSubKeyword());
        System.out.println("installAdvertiserSubPlacement(): "  + rawLog.installAdvertiserSubPlacement());
        System.out.println("installAdvertiserSubPublisher(): "  + rawLog.installAdvertiserSubPublisher());
        System.out.println("installAdvertiserSubSite(): "  + rawLog.installAdvertiserSubSite());
        System.out.println("installAgencyId(): "  + rawLog.installAgencyId());
        System.out.println("installAttributed(): "  + rawLog.installAttributed());
        System.out.println("installCreated(): "  + rawLog.installCreated());
        System.out.println("installDate(): "  + rawLog.installDate());
        System.out.println("installDateHour(): "  + rawLog.installDateHour());
        System.out.println("installId(): "  + rawLog.installId());
        System.out.println("installIdDate(): "  + rawLog.installIdDate());
        System.out.println("installIdUnique1(): "  + rawLog.installIdUnique1());
        System.out.println("installIdUnique2(): "  + rawLog.installIdUnique2());
        System.out.println("installIntervalDay(): "  + rawLog.installIntervalDay());
        System.out.println("installIntervalMonth(): "  + rawLog.installIntervalMonth());
        System.out.println("installIntervalWeek(): "  + rawLog.installIntervalWeek());
        System.out.println("installIntervalYear(): "  + rawLog.installIntervalYear());
        System.out.println("installPublisherId(): "  + rawLog.installPublisherId());
        System.out.println("installPublisherSubAd(): "  + rawLog.installPublisherSubAd());
        System.out.println("installPublisherSubAdgroup(): "  + rawLog.installPublisherSubAdgroup());
        System.out.println("installPublisherSubCampaign(): "  + rawLog.installPublisherSubCampaign());
        System.out.println("installPublisherSubKeyword(): "  + rawLog.installPublisherSubKeyword());
        System.out.println("installPublisherSubPlacement(): "  + rawLog.installPublisherSubPlacement());
        System.out.println("installPublisherSubPublisher(): "  + rawLog.installPublisherSubPublisher());
        System.out.println("installPublisherSubSite(): "  + rawLog.installPublisherSubSite());
        System.out.println("installReferrer(): "  + rawLog.installReferrer());
        System.out.println("installStatClickId(): "  + rawLog.installStatClickId());
        System.out.println("installStatImpressionId(): "  + rawLog.installStatImpressionId());
        System.out.println("installViewThrough(): "  + rawLog.installViewThrough());
        System.out.println("installs(): "  + rawLog.installs());
        System.out.println("iosAdTracking(): "  + rawLog.iosAdTracking());
        System.out.println("iosIfa(): "  + rawLog.iosIfa());
        System.out.println("iosIfaMd5(): "  + rawLog.iosIfaMd5());
        System.out.println("iosIfaSha1(): "  + rawLog.iosIfaSha1());
        System.out.println("iosIfv(): "  + rawLog.iosIfv());
        System.out.println("iosIfvMd5(): "  + rawLog.iosIfvMd5());
        System.out.println("iosIfvSha1(): "  + rawLog.iosIfvSha1());
        System.out.println("ip(): "  + rawLog.ip());
        System.out.println("isAssist(): "  + rawLog.isAssist());
        System.out.println("isCoppa(): "  + rawLog.isCoppa());
        System.out.println("isFirst(): "  + rawLog.isFirst());
        System.out.println("isFirstPurchase(): "  + rawLog.isFirstPurchase());
        System.out.println("isLast(): "  + rawLog.isLast());
        System.out.println("isNonwindowedAttribution(): "  + rawLog.isNonwindowedAttribution());
        System.out.println("isPaid(): "  + rawLog.isPaid());
        System.out.println("isPurchase(): "  + rawLog.isPurchase());
        System.out.println("isReengagement(): "  + rawLog.isReengagement());
        System.out.println("isUnique(): "  + rawLog.isUnique());
        System.out.println("isViewThrough(): "  + rawLog.isViewThrough());
        System.out.println("language(): "  + rawLog.language());
        System.out.println("languageId(): "  + rawLog.languageId());
        System.out.println("lastOpenDatetime(): "  + rawLog.lastOpenDatetime());
        System.out.println("lastReengagementPublisherId(): "  + rawLog.lastReengagementPublisherId());
        System.out.println("lastReengagementStatEventId(): "  + rawLog.lastReengagementStatEventId());
        System.out.println("lastReengagementStatOpenId(): "  + rawLog.lastReengagementStatOpenId());
        System.out.println("lastReferralUrl(): "  + rawLog.lastReferralUrl());
        System.out.println("lastStatOpenId(): "  + rawLog.lastStatOpenId());
        System.out.println("latitude(): "  + rawLog.latitude());
        System.out.println("level(): "  + rawLog.level());
        System.out.println("logCreated(): "  + rawLog.logCreated());
        System.out.println("logDateHour(): "  + rawLog.logDateHour());
        System.out.println("logId(): "  + rawLog.logId());
        System.out.println("logIdDate(): "  + rawLog.logIdDate());
        System.out.println("logIdUnique1(): "  + rawLog.logIdUnique1());
        System.out.println("logIdUnique2(): "  + rawLog.logIdUnique2());
        System.out.println("logStatus(): "  + rawLog.logStatus());
        System.out.println("logType(): "  + rawLog.logType());
        System.out.println("longitude(): "  + rawLog.longitude());
        System.out.println("macAddress(): "  + rawLog.macAddress());
        System.out.println("macAddressMd5(): "  + rawLog.macAddressMd5());
        System.out.println("macAddressSha1(): "  + rawLog.macAddressSha1());
        System.out.println("macAddressSha256(): "  + rawLog.macAddressSha256());
        System.out.println("matId(): "  + rawLog.matId());
        System.out.println("matchType(): "  + rawLog.matchType());
        System.out.println("mobileAppType(): "  + rawLog.mobileAppType());
        System.out.println("modified(): "  + rawLog.modified());
        System.out.println("odin(): "  + rawLog.odin());
        System.out.println("openUdid(): "  + rawLog.openUdid());
        System.out.println("opens(): "  + rawLog.opens());
        System.out.println("osId(): "  + rawLog.osId());
        System.out.println("osIdMd5(): "  + rawLog.osIdMd5());
        System.out.println("osIdSha1(): "  + rawLog.osIdSha1());
        System.out.println("osIdSha256(): "  + rawLog.osIdSha256());
        System.out.println("osJailbroke(): "  + rawLog.osJailbroke());
        System.out.println("osVersion(): "  + rawLog.osVersion());
        System.out.println("packageName(): "  + rawLog.packageName());
        System.out.println("payoutType(): "  + rawLog.payoutType());
        System.out.println("pi2(): "  + rawLog.pi2());
        System.out.println("pi3(): "  + rawLog.pi3());
        System.out.println("pi4(): "  + rawLog.pi4());
        System.out.println("pi5(): "  + rawLog.pi5());
        System.out.println("platform(): "  + rawLog.platform());
        System.out.println("publisherAdgroupId(): "  + rawLog.publisherAdgroupId());
        System.out.println("publisherAdgroupItemId(): "  + rawLog.publisherAdgroupItemId());
        System.out.println("publisherId(): "  + rawLog.publisherId());
        System.out.println("publisherLookbackHours(): "  + rawLog.publisherLookbackHours());
        System.out.println("publisherRefId(): "  + rawLog.publisherRefId());
        System.out.println("publisherSubAd(): "  + rawLog.publisherSubAd());
        System.out.println("publisherSubAdId(): "  + rawLog.publisherSubAdId());
        System.out.println("publisherSubAdgroup(): "  + rawLog.publisherSubAdgroup());
        System.out.println("publisherSubAdgroupId(): "  + rawLog.publisherSubAdgroupId());
        System.out.println("publisherSubCampaign(): "  + rawLog.publisherSubCampaign());
        System.out.println("publisherSubCampaignId(): "  + rawLog.publisherSubCampaignId());
        System.out.println("publisherSubKeyword(): "  + rawLog.publisherSubKeyword());
        System.out.println("publisherSubKeywordId(): "  + rawLog.publisherSubKeywordId());
        System.out.println("publisherSubPlacement(): "  + rawLog.publisherSubPlacement());
        System.out.println("publisherSubPlacementId(): "  + rawLog.publisherSubPlacementId());
        System.out.println("publisherSubPublisher(): "  + rawLog.publisherSubPublisher());
        System.out.println("publisherSubPublisherId(): "  + rawLog.publisherSubPublisherId());
        System.out.println("publisherSubSite(): "  + rawLog.publisherSubSite());
        System.out.println("publisherSubSiteId(): "  + rawLog.publisherSubSiteId());
        System.out.println("publisherSub1(): "  + rawLog.publisherSub1());
        System.out.println("publisherSub2(): "  + rawLog.publisherSub2());
        System.out.println("publisherSub3(): "  + rawLog.publisherSub3());
        System.out.println("publisherSub4(): "  + rawLog.publisherSub4());
        System.out.println("publisherSub5(): "  + rawLog.publisherSub5());
        System.out.println("publisherUserId(): "  + rawLog.publisherUserId());
        System.out.println("purchaseStatus(): "  + rawLog.purchaseStatus());
        System.out.println("purchaseValidationStatus(): "  + rawLog.purchaseValidationStatus());
        System.out.println("pv2(): "  + rawLog.pv2());
        System.out.println("pv3(): "  + rawLog.pv3());
        System.out.println("pv4(): "  + rawLog.pv4());
        System.out.println("pv5(): "  + rawLog.pv5());
        System.out.println("quantity(): "  + rawLog.quantity());
        System.out.println("rating(): "  + rawLog.rating());
        System.out.println("reachTotal(): "  + rawLog.reachTotal());
        System.out.println("reengagementAdNetworkId(): "  + rawLog.reengagementAdNetworkId());
        System.out.println("reengagementAdvertiserSubAd(): "  + rawLog.reengagementAdvertiserSubAd());
        System.out.println("reengagementAdvertiserSubAdgroup(): "  + rawLog.reengagementAdvertiserSubAdgroup());
        System.out.println("reengagementAdvertiserSubCampaign(): "  + rawLog.reengagementAdvertiserSubCampaign());
        System.out.println("reengagementAdvertiserSubKeyword(): "  + rawLog.reengagementAdvertiserSubKeyword());
        System.out.println("reengagementAdvertiserSubPlacement(): "  + rawLog.reengagementAdvertiserSubPlacement());
        System.out.println("reengagementAdvertiserSubPublisher(): "  + rawLog.reengagementAdvertiserSubPublisher());
        System.out.println("reengagementAdvertiserSubSite(): "  + rawLog.reengagementAdvertiserSubSite());
        System.out.println("reengagementAgencyId(): "  + rawLog.reengagementAgencyId());
        System.out.println("reengagementAttributableCreated(): "  + rawLog.reengagementAttributableCreated());
        System.out.println("reengagementAttributableDateHour(): "  + rawLog.reengagementAttributableDateHour());
        System.out.println("reengagementAttributableId(): "  + rawLog.reengagementAttributableId());
        System.out.println("reengagementAttributableIdDate(): "  + rawLog.reengagementAttributableIdDate());
        System.out.println("reengagementAttributableIdUnique1(): "  + rawLog.reengagementAttributableIdUnique1());
        System.out.println("reengagementAttributableIdUnique2(): "  + rawLog.reengagementAttributableIdUnique2());
        System.out.println("reengagementAttributableIntervalDay(): "  + rawLog.reengagementAttributableIntervalDay());
        System.out.println("reengagementAttributableIntervalMonth(): "  + rawLog.reengagementAttributableIntervalMonth());
        System.out.println("reengagementAttributableIntervalWeek(): "  + rawLog.reengagementAttributableIntervalWeek());
        System.out.println("reengagementAttributableIntervalYear(): "  + rawLog.reengagementAttributableIntervalYear());
        System.out.println("reengagementAttributableType(): "  + rawLog.reengagementAttributableType());
        System.out.println("reengagementAttributed(): "  + rawLog.reengagementAttributed());
        System.out.println("reengagementCreated(): "  + rawLog.reengagementCreated());
        System.out.println("reengagementDateHour(): "  + rawLog.reengagementDateHour());
        System.out.println("reengagementId(): "  + rawLog.reengagementId());
        System.out.println("reengagementIdDate(): "  + rawLog.reengagementIdDate());
        System.out.println("reengagementIdUnique1(): "  + rawLog.reengagementIdUnique1());
        System.out.println("reengagementIdUnique2(): "  + rawLog.reengagementIdUnique2());
        System.out.println("reengagementIntervalDay(): "  + rawLog.reengagementIntervalDay());
        System.out.println("reengagementIntervalMonth(): "  + rawLog.reengagementIntervalMonth());
        System.out.println("reengagementIntervalWeek(): "  + rawLog.reengagementIntervalWeek());
        System.out.println("reengagementIntervalYear(): "  + rawLog.reengagementIntervalYear());
        System.out.println("reengagementPublisherId(): "  + rawLog.reengagementPublisherId());
        System.out.println("reengagementPublisherSubAd(): "  + rawLog.reengagementPublisherSubAd());
        System.out.println("reengagementPublisherSubAdgroup(): "  + rawLog.reengagementPublisherSubAdgroup());
        System.out.println("reengagementPublisherSubCampaign(): "  + rawLog.reengagementPublisherSubCampaign());
        System.out.println("reengagementPublisherSubKeyword(): "  + rawLog.reengagementPublisherSubKeyword());
        System.out.println("reengagementPublisherSubPlacement(): "  + rawLog.reengagementPublisherSubPlacement());
        System.out.println("reengagementPublisherSubPublisher(): "  + rawLog.reengagementPublisherSubPublisher());
        System.out.println("reengagementPublisherSubSite(): "  + rawLog.reengagementPublisherSubSite());
        System.out.println("reengagementType(): "  + rawLog.reengagementType());
        System.out.println("reengagementViewThrough(): "  + rawLog.reengagementViewThrough());
        System.out.println("reengagementviewThrough(): "  + rawLog.reengagementviewThrough());
        System.out.println("referralSource(): "  + rawLog.referralSource());
        System.out.println("referralUrl(): "  + rawLog.referralUrl());
        System.out.println("regionCode(): "  + rawLog.regionCode());
        System.out.println("regionId(): "  + rawLog.regionId());
        System.out.println("requestUrl(): "  + rawLog.requestUrl());
        System.out.println("revenues(): "  + rawLog.revenues());
        System.out.println("revenuesUsd(): "  + rawLog.revenuesUsd());
        System.out.println("revenue(): "  + rawLog.revenue());
        System.out.println("revenueUsd(): "  + rawLog.revenueUsd());
        System.out.println("sdk(): "  + rawLog.sdk());
        System.out.println("sdkPlugin(): "  + rawLog.sdkPlugin());
        System.out.println("sdkVersion(): "  + rawLog.sdkVersion());
        System.out.println("searchString(): "  + rawLog.searchString());
        System.out.println("sessionDatetime(): "  + rawLog.sessionDatetime());
        System.out.println("sessionIp(): "  + rawLog.sessionIp());
        System.out.println("siteEventId(): "  + rawLog.siteEventId());
        System.out.println("siteEventItemsCount(): "  + rawLog.siteEventItemsCount());
        System.out.println("siteEventOffset(): "  + rawLog.siteEventOffset());
        System.out.println("siteEventRef(): "  + rawLog.siteEventRef());
        System.out.println("siteEventType(): "  + rawLog.siteEventType());
        System.out.println("siteId(): "  + rawLog.siteId());
        System.out.println("siteOffset(): "  + rawLog.siteOffset());
        System.out.println("sourceFile(): "  + rawLog.sourceFile());
        System.out.println("statApprovedId(): "  + rawLog.statApprovedId());
        System.out.println("statClickId(): "  + rawLog.statClickId());
        System.out.println("statEventId(): "  + rawLog.statEventId());
        System.out.println("statImpressionId(): "  + rawLog.statImpressionId());
        System.out.println("statInstallId(): "  + rawLog.statInstallId());
        System.out.println("statOpenId(): "  + rawLog.statOpenId());
        System.out.println("statUpdateId(): "  + rawLog.statUpdateId());
        System.out.println("status(): "  + rawLog.status());
        System.out.println("statusCode(): "  + rawLog.statusCode());
        System.out.println("systemDate(): "  + rawLog.systemDate());
        System.out.println("testProfileId(): "  + rawLog.testProfileId());
        System.out.println("tpid(): "  + rawLog.tpid());
        System.out.println("trackingId(): "  + rawLog.trackingId());
        System.out.println("transactionId(): "  + rawLog.transactionId());
        System.out.println("twitterAttribution(): "  + rawLog.twitterAttribution());
        System.out.println("twitterEngagementDatetime(): "  + rawLog.twitterEngagementDatetime());
        System.out.println("twitterNonwindowedAttribution(): "  + rawLog.twitterNonwindowedAttribution());
        System.out.println("twitterUserId(): "  + rawLog.twitterUserId());
        System.out.println("twitterViewImpression(): "  + rawLog.twitterViewImpression());
        System.out.println("unid(): "  + rawLog.unid());
        System.out.println("unverifiedRevenues(): "  + rawLog.unverifiedRevenues());
        System.out.println("unverifiedRevenuesUsd(): "  + rawLog.unverifiedRevenuesUsd());
        System.out.println("updates(): "  + rawLog.updates());
        System.out.println("userAgent(): "  + rawLog.userAgent());
        System.out.println("userEmail(): "  + rawLog.userEmail());
        System.out.println("userEmailMd5(): "  + rawLog.userEmailMd5());
        System.out.println("userEmailSha1(): "  + rawLog.userEmailSha1());
        System.out.println("userEmailSha256(): "  + rawLog.userEmailSha256());
        System.out.println("userId(): "  + rawLog.userId());
        System.out.println("userIdMd5(): "  + rawLog.userIdMd5());
        System.out.println("userIdSha1(): "  + rawLog.userIdSha1());
        System.out.println("userPhone(): "  + rawLog.userPhone());
        System.out.println("userPhoneMd5(): "  + rawLog.userPhoneMd5());
        System.out.println("userPhoneSha1(): "  + rawLog.userPhoneSha1());
        System.out.println("userPhoneSha256(): "  + rawLog.userPhoneSha256());
        System.out.println("username(): "  + rawLog.username());
        System.out.println("usernameMd5(): "  + rawLog.usernameMd5());
        System.out.println("usernameSha1(): "  + rawLog.usernameSha1());
        System.out.println("usernameSha256(): "  + rawLog.usernameSha256());
        System.out.println("viewMax(): "  + rawLog.viewMax());
        System.out.println("viewPercent(): "  + rawLog.viewPercent());
        System.out.println("viewThrough(): "  + rawLog.viewThrough());
        System.out.println("windowsAid(): "  + rawLog.windowsAid());
        System.out.println("windowsAidMd5(): "  + rawLog.windowsAidMd5());
        System.out.println("windowsAidSha1(): "  + rawLog.windowsAidSha1());
        System.out.println("wurflBrandName(): "  + rawLog.wurflBrandName());
        System.out.println("wurflDeviceOs(): "  + rawLog.wurflDeviceOs());
        System.out.println("wurflDeviceOsVersion(): "  + rawLog.wurflDeviceOsVersion());
        System.out.println("wurflModelName(): "  + rawLog.wurflModelName());
*/
        
    }

    // This method is not completed.
   /* private void createGetterCache(File file) {
        String funClass = "com.tune.schema.RawLog";
        Class<?> c;
                   
        try {
            c = Class.forName(funClass);
            o = c.newInstance();
            List<String> lines = Files.readLines(file, Charsets.UTF_8);
            for (String line : lines) {
                String[] items = line.split(":");
                if (items.length == 2) {
                    String key = items[0].replaceAll("[\\s+;]","");
                    String value = items[1].replaceAll("[\\s+;]","");                    
                    String getterName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, key);
                    Method m = c.getMethod(getterName, null);
                    getterMap.put(key, m);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        }
    }
*/
   
   

}

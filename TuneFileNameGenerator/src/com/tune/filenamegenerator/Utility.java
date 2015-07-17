package com.tune.filenamegenerator;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class Utility {
	
	
	/*This method filter out the files which are needed form the folders that are generated in the folders list.
	 * Stores the filtered required filenames to the files arraylist.*/	
	
	
	public static List<String> getFiles(List<String> folders, List<String> patterns, FileSystem fs) 
	{
		ArrayList<String> foldersNotFound = new ArrayList<String>();
		List<String> files = new ArrayList<String>();
		for(String f: folders)
		{
			Path folder =new Path(f);	
			FileStatus[] status;
			try {
				status = fs.listStatus(folder);
				for(int p=0; p<status.length; p++ )
				{
					String file = status[p].getPath().getName();
					System.out.println("file is: "+file);
					for ( int s =0 ; s< patterns.size(); s++)
					{
						if(file.matches(patterns.get(s)))
						{
							System.out.println("pattern file is: "+file);
							files.add(f+"/"+file);
							s= patterns.size();
						}
					}
					
				}
				//System.out.println("file size :"+files.size());
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				foldersNotFound.add(folder.toString());
				System.out.println("File "+folder+"not found");
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			finally{
				System.out.println("These are the folders that were not found");
				for( int w=0; w<foldersNotFound.size(); w++)
				{
					System.out.println(foldersNotFound.get(w));
					
				}
			}	
		}
		return files;
	}
	
	/**
	 * 
	 * @param revisionId
	 * @param producerId
	 * @param sequenceNumber
	 * @param advertiserId
	 * @param prisonId
	 * @return
	 *This method takes in the JSON parameters and 
	 * generates the regex pattern to be used in the file parsing/filtering stage.
	 * Returns a list of all the possible regexes to match based on the JSON parameters passed to it.*/
	
	
	public static List<String> getPatterns( String revisionId,
			List<Integer> producerId, String sequenceNumber,
			List<Integer> advertiserId, List<Integer> prisonId) 
	{
		List<String> patterns = new ArrayList<String>();
		for ( int h = 0; h <advertiserId.size(); h++)
	  {
		String shardId= String.valueOf((advertiserId.get(h))%21);
		System.out.println("Shard_id :"+shardId);
		StringBuilder patternName= new StringBuilder();
		
		//TODO : Right now we are returning every revision which is less than the specified revisionId. A filtering needs to be done so that it returns files with the highest revision number.
			
		if( prisonId.size()==0  && producerId.size()==0) //No prisonIds and no producerIds given.
			{
				patternName.append("shard"+shardId+"_\\d{8}_rev\\d{1,2}_prison\\d{2}_batcher\\d{2}_seq\\d{1,2}.fb.gz");
				System.out.println("Pattern: "+patternName.toString());
				patterns.add(patternName.toString());
				patternName.setLength(0);
			}
			else if(prisonId.size()==0 && producerId.size()!=0) //No prisonId.
			{	
				for(int i=0 ; i < producerId.size(); i++)
				{
					String producer= String.format("%02d", producerId.get(i)); 
					patternName.append("shard"+shardId+"_\\d{8}_rev\\d{1,2}_prison\\d{2}_batcher"+producer+"_seq\\d{1,2}.fb.gz");
					System.out.println("Pattern: "+patternName.toString());
					patterns.add(patternName.toString());
					patternName.setLength(0);
				}
		   }
			else if( prisonId.size()!=0 && producerId.size()==0) // No producerIds(batchers.)
			{
				for(int i=0 ; i < prisonId.size(); i++)
				{
					String prison= String.format("%02d", prisonId.get(i)); 
					patternName.append("shard"+shardId+"_\\d{8}_rev\\d{1,2}_prison"+prison+"_batcher\\d{1,2}_seq\\d{1,2}.fb.gz");
					System.out.println("Pattern: "+patternName.toString());
					patterns.add(patternName.toString());
					patternName.setLength(0);
				}
			}
			else
			{
				for(int i=0 ; i < prisonId.size(); i++) //Given prisonIds and producerIds.
				{
					String prison= String.format("%02d", prisonId.get(i)); 
					for(int j=0 ; j < producerId.size(); j++)
					{
						String producer= String.format("%02d", producerId.get(j)); 
						patternName.append("shard"+shardId+"_\\d{8}_rev\\d{1,2}_prison"+prison+"_batcher"+producer+"_seq\\d{1,2}.fb.gz");
						System.out.println("Pattern: "+patternName.toString());
						patterns.add(patternName.toString());
						patternName.setLength(0);
					}
				}
			}
		System.out.println("Set the length to o");
			patternName.setLength(0);
		}
		return patterns;
	}

	/**
	 * 
	 * @param startdate
	 * @param enddate
	 * @return
	 * This method generates the folder names according to the start and end date 
	 * provided as MIN and MAX in the JSON.
	 * It creates the folder names that actually exists in the HDFS or s3 buckets. This folders will contain the file that are requested.
	 */
public static List<String> getFolderNames( Date startdate, Date enddate) {
		
		List<String> folders= new ArrayList<String>();
		List<Date> dates = new ArrayList<Date>();
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(startdate);
		 
		while( (calendar.getTime().before(enddate)) || calendar.getTime().equals(enddate))
		{
			Date resultDate = calendar.getTime();
			dates.add(resultDate);
			String folderName = "/authoritative/"+resultDate.getYear()+"/"+String.format("%02d",(resultDate.getMonth()+1))+"/"+String.format("%02d",resultDate.getDate());
			System.out.println(folderName);
			calendar.add(Calendar.DATE,1);
			folders.add(folderName);
		}
		return folders;
	}
		

}

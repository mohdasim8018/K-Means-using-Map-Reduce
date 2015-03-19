import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;


/*
 * Driver class  
 */

public class KDriver {
	private HashMap<String, String> centroidList = new HashMap<String,String>();
	private int jobCount = 1;
	//public String CENTROID_FILE_NAME = "/centroid.txt";

	public static void main(String[] args) throws IOException, ParseException {
		
		KDriver obj = new KDriver();
		
		//Configuration conf = getConf();
		//JobConf conf1 = new JobConf();
		
		
		
		//BufferedReader bfr=new BufferedReader(new InputStreamReader(hdfs.open(newFilePath)));
		
        
        //Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
        
        //BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		
		//String centroidFile = "/user/root/input1/centroid";
		int linescount = 0;
		
		if(args.length == 2){
			/*
			 * Read the centroid file into a Hashmap
			 */
			
			
			try{
				Path pt=new Path("/user/root/input1/centroid.txt");
		        FileSystem fs = FileSystem.get(new Configuration());
		        BufferedReader bufferReader = new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
	
		        while ((line = bufferReader.readLine()) != null)   {
		        	linescount = linescount+1;
		        	String []keyVal = line.split("\t");
		      		obj.centroidList.put(keyVal[0], keyVal[1]);
		        }
		        bufferReader.close();
		    }
		    catch(Exception e){
		    	System.out.println("Error while reading file line by line:" + e.getMessage());                      
		    }
			
			/*
			 * 	Iterative jobs for the convergence of the centroids, meaning that
			 *	the change of centroids between current iteration and last iteration is less
			 *	than 0.01)
			 */
			int iterationCount = 0;						
			 
			if(linescount >=3 && linescount <=8){
				while(obj.jobCount != 0){
					JobClient client2 = new JobClient();
					JobConf conf2 = new JobConf(KDriver.class);
					
					conf2.setMapperClass(KMapper.class);
					conf2.setPartitionerClass(KPartitioner.class);
					conf2.setReducerClass(KReducer.class);
					conf2.setNumReduceTasks(3);
				
					//Set the output types for mapper and reducer Class
					conf2.setMapOutputKeyClass(Text.class);
					conf2.setMapOutputValueClass(Text.class);
					conf2.setOutputKeyClass(Text.class);
					conf2.setOutputValueClass(Text.class);
					
					String input, output;	
						
					input = args[0];	
					
					output = args[1] + (iterationCount + 1);		
					FileInputFormat.setInputPaths(conf2, new Path(input));
					FileOutputFormat.setOutputPath(conf2, new Path(output));
					client2.setConf(conf2);
						
					try {
						JobClient.runJob(conf2);
					} 
					catch (Exception e) {
						e.printStackTrace();
					}
					
					obj.jobCount--;
					
					
					HashMap<String, String> newCentroidList = new HashMap<String,String>();
					
					
					try{   	
						//copyMerge(FileSystem srcFS,Path srcDir,FileSystem dstFS, Path dstFile, boolean deleteSource,conf2, String addString)
						String src = "/user/root/"+output;
						//String temp = "out"+iterationCount;
						String dest = "/user/root/"+output+"/final.txt";
						//String dest = "/user/root/"+output;
						Path pt=new Path(src);
						Path dstFile = new Path(dest);
				        FileSystem fs = FileSystem.get(conf2);
				        FileUtil.copyMerge(fs,pt,fs, dstFile, false,conf2, null);
				        //BufferedReader bufferReader = new BufferedReader(new InputStreamReader(fs.open(dstFile)));
				        BufferedReader bufferReader = new BufferedReader(new InputStreamReader(fs.open(dstFile), "UTF-8"));
				        String line;
				        while ((line = bufferReader.readLine()) != null) {
				        	String []keyVal = line.split("\t");
				      		newCentroidList.put(keyVal[0], keyVal[1]);
				        }
				        bufferReader.close();
				    }
				    catch(Exception e){
				    	System.out.println("Error while reading file line by line:" + e.getMessage());                      
				    }	
					
					
					/*
					 * Check if the the change of centroid from the current iteration 
					 * is greater than 0.01 to continue the iteration
					 */
					int counter=0;
					for(Map.Entry<String, String> entry : newCentroidList.entrySet()){
						String result = obj.centroidList.getOrDefault(entry.getKey(), "N");
						if(!(result.equalsIgnoreCase("N"))){
							String[] newVal = entry.getValue().split(",");
							String[] oldVal = result.split(",");
							Double sum =0.0;
							Double Edist = 0.0;
							for(int i =0; i<oldVal.length; i++){
								double x1 = Double.parseDouble(newVal[i]);
								double y1 = Double.parseDouble(oldVal[i]);
								double  xDiff = x1-y1;
						        double  xSqr  = Math.pow(xDiff, 2);
								sum+=xSqr;
							}
							Edist = Math.sqrt(sum);
							if(Edist>0.01){
								counter++;
							}
						}
					}
					if(counter >0){
						System.out.println("Yes");
						obj.centroidList.clear();
						Path pt1=new Path("/user/root/input1/centroid.txt");
				        FileSystem fs = FileSystem.get(new Configuration());
						
				        
				        ArrayList<String> sb=new ArrayList<String>();
				        FSDataOutputStream fsOutStream = fs.create(pt1, true);
				        BufferedWriter br = new BufferedWriter( new OutputStreamWriter(fsOutStream, "UTF-8" ) );
				        
						for(Map.Entry<String, String> entry1 : newCentroidList.entrySet()){
							obj.centroidList.put(entry1.getKey(), entry1.getValue());
							sb.add(entry1.getKey()+"\t"+entry1.getValue());
						}
						for(String temp:sb){
							br.write(temp+"\n");
						}
						
						br.close();
						
						obj.jobCount++;
					}
					iterationCount++;
				}
				Path pt2=new Path("/user/root/input1/centroid.txt");
				FileSystem fs = FileSystem.get(new Configuration());
				FSDataOutputStream fsOutStream = fs.create(pt2, true);
				BufferedWriter br = new BufferedWriter( new OutputStreamWriter(fsOutStream, "UTF-8" ) );
				//br.write("Hello World");
				br.write("C1	23357,401753,229671,826166,670144,946988,255137,89322,361894,828360"+"\n");
				br.write("C2	167909,260564,428079,837156,182538,817263,439382,135960,263912,64869"+"\n");
				br.write("C3	46153,400715,236515,818781,671558,914860,271018,91979,376398,821506"+"\n");
				br.write("C4	53709,374748,246537,791761,657181,918538,276855,88861,357334,845216"+"\n");
				br.write("C5	53717,379826,219550,800236,654848,937875,266376,79485,363165,823282"+"\n");
				br.close();
			}
			else
			{
				System.out.println("Atleast 3 and a maximum of 8 cenroids allowed!!! ");
			}
		}
		else
		{
			System.out.println("Invalid command line arguments!!!");
		}
	}

}

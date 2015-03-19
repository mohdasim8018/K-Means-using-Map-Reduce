import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
/*
 * Mapper Class
 */
public class KMapper extends MapReduceBase
				implements Mapper<LongWritable, Text, Text,Text>{
	
	/*
	 * Map function:
	 * 	Parse each data point of the input file and take euclidian distance
	 * 	against each centroid in the centroid file
	 */
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
		
		HashMap<String, Double> nearList = new HashMap<String, Double>();
		HashMap<String, String> centroidList = new HashMap<String,String>();
		
		Text kKey = new Text();
		Text kValue = new Text();
		String []keyVal = value.toString().split("\t");
		String []values = keyVal[1].split(",");
		
		Path pt=new Path("/user/root/input1/centroid.txt");
        FileSystem fs = FileSystem.get(new Configuration());
		
		try{   	
	        BufferedReader bufferReader = new BufferedReader(new InputStreamReader(fs.open(pt)));
	        String line;

	        while ((line = bufferReader.readLine()) != null) {
	        	String []keyValR = line.split("\t");
	      		centroidList.put(keyValR[0], keyValR[1]);
	      		System.out.println(keyValR[1]);
	        }
	        bufferReader.close();
	    }
	    catch(Exception e){
	    	System.out.println("Error while reading file line by line:" + e.getMessage());                      
	    }
		
		/*
		 * Calculate euclidian distance of the data point against 
		 * each centroid and store each calculated distance in
		 * a hashmap
		 */
		for (Map.Entry<String, String> entry : centroidList.entrySet()) {
			Double Edist = 0.0;
			Double sum =0.0;
			String[] Cvalue= entry.getValue().split(",");
			for(int i =0; i<values.length; i++){
				//nf.parse(newVal[i]).doubleValue();
				double x1=0.0;
				double y1=0.0;
				x1 = Double.parseDouble(values[i]);
				y1 = Double.parseDouble(Cvalue[i]);
				double  xDiff = x1-y1;
		        double  xSqr  = Math.pow(xDiff, 2);
				sum+=xSqr;
			}
			Edist = Math.sqrt(sum);
			nearList.put(entry.getKey(),Edist);
		}
		
		/*
		 * Get the least distance centroid 
		 */
		Double min = Double.MAX_VALUE; 
		String keyFinal = null;
		for (Map.Entry<String, Double> entry : nearList.entrySet()) {
			if(entry.getValue() < min){
				min = entry.getValue();
				keyFinal = entry.getKey();
			}
		}
		kKey.set(keyFinal);
		kValue.set(keyVal[1].toString());
		output.collect(kKey, kValue);
	}

}

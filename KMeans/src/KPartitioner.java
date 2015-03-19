import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/*
 * Partitioner Class
 */
public class KPartitioner implements Partitioner<Text, Text>{

	/*
	 * Create three partitions of the data points
	 */
	public int getPartition(Text key, Text value, int numReduceTasks) {
		String val = key.toString();
		String subKey = val.substring(1, val.length());
		int nodeInt = Integer.parseInt(subKey);

		 //This is done to avoid performing mod with 0
		 if(numReduceTasks == 0)
			 return 0;
		 
		 //If the nodeId is <20, assign partition 0
		 if(nodeInt <=20){
			 return 0;
		 }
		 
		 //Else if the node ID is between 20 and 50, assign partition 1
		 if(nodeInt >20 && nodeInt <=50){
			 return 1 % numReduceTasks;
		 }
		 
		 //Otherwise assign partition 2
		 else
			 return 2 % numReduceTasks;
	}
	
	public void configure(JobConf arg0) {
		
	}
	
}

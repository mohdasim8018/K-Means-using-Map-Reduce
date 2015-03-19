import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*; 


/*
 * Reducer Class
 */
public class KReducer extends MapReduceBase implements Reducer<Text, Text, Text,
Text>{

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
		Double[] attr = new Double[10];
		
		for(int i=0; i<10; i++){
			attr[i] = 0.0;
		}
		
		Text kValue = new Text();
		
		Double counter = 0.0;
		StringBuilder valFinal = new StringBuilder();
		
		while(values.hasNext()){
			counter++;
			String[] list = values.next().toString().split(",");
			for(int i=0; i<list.length;i++){
				attr[i]=attr[i]+Double.parseDouble(list[i]);
			}
		}
		
		
		for(int i=0;i<attr.length;i++){
			attr[i] = attr[i]/counter;
			valFinal.append(attr[i].toString()+",");
		}
		
		String valFS = valFinal.toString(); 
		valFS = valFS.substring(0, valFS.length()-1);
		kValue.set(valFS);
		output.collect(key, kValue);
	}

}

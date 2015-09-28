package task1;


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TaskReducer extends Reducer<CompositeKey,Text,NullWritable,Text> {

	// we need ordered list
	private ArrayList<String> listA = new ArrayList<String>();
	private String listB = "";
	
	private static final NullWritable nullKey = NullWritable.get();
	
	public void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		listA.clear();
		listB="";
		
		

		for(Text tmp : values){		
			if(tmp.charAt(0)=='B'){	// will looking for char B in the beginning, added from mapper
				listB=tmp.toString().substring(1); // and will remove it for the final result
			} else {
				listA.add(tmp.toString());
			}
		}
			
			
		// Join
			for(String aVal:listA)
			{
				StringBuilder sb = new StringBuilder().append(aVal).append(", ").append(listB);
				//will set nullKey to remove join key from result set
				context.write(nullKey, new Text(sb.toString()));								
			}
		}	
}

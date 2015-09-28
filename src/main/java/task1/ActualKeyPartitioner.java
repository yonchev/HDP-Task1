package task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ActualKeyPartitioner extends Partitioner<CompositeKey,Text>{

	
	@Override
	public int getPartition(CompositeKey key, Text value, int numPartitions) {

		int partition = 0;
		// will split for second output after 7-1-2008 which getTime() long is 1199664000000L
		if(key.getDateFrom() > 1199664000000L )  partition = (int) (1 % numPartitions);
		return partition;		
	}

}

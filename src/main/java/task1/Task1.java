package task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Task1 {

	
	public static void main(String[] args) throws Exception {

		String separator = ",";
		String keyIndex = "0";
		
		String inPathOne = args[0];
		String inPathTwo = args[1];
		String outPath = args[2];		
		
		Configuration conf = new Configuration();
		conf.set("keyIndex", keyIndex);
		conf.set("separator", separator);
		
		Job job = Job.getInstance(conf);
		
		job.setNumReduceTasks(2);
		job.setJobName("flightSort");
		job.setMapperClass(TaskMapper.class);
		job.setReducerClass(TaskReducer.class);
		job.setMapOutputKeyClass(CompositeKey.class);
		job.setPartitionerClass(ActualKeyPartitioner.class);
		job.setGroupingComparatorClass(ActualKeyGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
		job.setJarByClass(Task1.class);
		
		FileInputFormat.setInputPaths(job, new Path(inPathOne), new Path(inPathTwo));
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}

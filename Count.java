import java.io.*;
//import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Count
{
	public static class CountMapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
			
			String val[]=value.toString().split(" ");
			String logtype=val[3];
			context.write(new Text(logtype),new IntWritable(1));
			
		}
	}
	
	
	
	public static class CountReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void reduce(Text k,Iterable<IntWritable> val,Context context) throws IOException,InterruptedException
		{
		int sum=0;
			for(IntWritable t:val)
			{			
			  sum=sum+t.get();
			}
	    context.write(k,new IntWritable(sum));
		}
	}
	
	public static void main(String args[]) throws Exception
	{
	Configuration conf=new Configuration();
	Job job=Job.getInstance(conf,"Log");
	
	job.setJarByClass(Count.class);
	job.setMapperClass(CountMapper.class);
	job.setReducerClass(CountReducer.class);
    
	job.setNumReduceTasks(1);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	
	FileInputFormat.addInputPath(job,new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	
	System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}



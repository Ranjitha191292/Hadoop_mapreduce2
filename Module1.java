import java.io.*;
//import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Module1
{
	public static class Module1Mapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
			
			String val[]=value.toString().split(" ");
			//String logtype=val[0];
			String module=val[1];
			context.write(new Text(module),new IntWritable(1));
			
		}
	}
	public class Module1Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text inpK,Iterable<IntWritable> inpV, Context context) throws IOException, InterruptedException{
			int sum=0;
			//int max=0;
				for(IntWritable t:inpV)
				{			
				  sum=sum+t.get();
				}
		    context.write(inpK,new IntWritable(sum));

	}
	}
	
	
	
	public static class Module1Reducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void reduce(Text k,Iterable<IntWritable> val,Context context) throws IOException,InterruptedException
		{
			int max=0;
			Text key=null;
		for(IntWritable t:val)
		{
			int each=t.get();
			if(max<each)
			{
				max=each;
			    key=k;
		    }
		}
		context.write(key,new IntWritable(max));
	}
	}
	
	public static void main(String args[]) throws Exception
	{
	Configuration conf=new Configuration();
	Job job=Job.getInstance(conf,"Module");
	
	job.setJarByClass(Module1.class);
	job.setMapperClass(Module1Mapper.class);
	job.setCombinerClass(Module1Combiner.class);
	job.setReducerClass(Module1Reducer.class);
    
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



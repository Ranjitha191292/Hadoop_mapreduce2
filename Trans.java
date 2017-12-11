import java.io.*;
//import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Trans
{
	public static class TransMap extends Mapper<LongWritable,Text,Text,FloatWritable>
	{
	public void map(LongWritable keys,Text values,Context context) throws IOException,InterruptedException
	{
		String str[]=values.toString().split(",");
	    String tran=str[0];
	    String amt=str[3];
	    float f=Float.parseFloat(amt);
	    context.write(new Text(tran),new FloatWritable(f));
	}
	} 
	
	public static class TransReduce extends Reducer<Text,FloatWritable,Text,IntWritable>
	{
	public void reduce(Text k,Iterable<FloatWritable> val,Context context) throws IOException,InterruptedException
	{
		
		int count=0;
		//int sum=0;
		for(FloatWritable ff:val)
		{
			if(ff.get()>175 && ff.get()<200)
			{
				
				count=count+1;
			
			}
			
		}
		context.write(k,new IntWritable(count));
	}
	}
	
	public static void main(String args[]) throws Exception
	{
	Configuration conf=new Configuration();
	Job job=Job.getInstance(conf,"Transaction");
	
	job.setJarByClass(Trans.class);
	job.setMapperClass(TransMap.class);
	job.setReducerClass(TransReduce.class);
	
	job.setNumReduceTasks(1);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(FloatWritable.class);
	
	FileInputFormat.addInputPath(job,new Path (args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	
	System.exit(job.waitForCompletion(true)? 0 : 1);
	}
	
	
	
	
	
	}
  

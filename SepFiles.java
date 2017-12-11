import java.io.*;
//import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class SepFiles
{
	public static class SepFilesMap extends Mapper<LongWritable,Text,Text,FloatWritable>
	{
	public void map(LongWritable keys,Text values,Context context) throws IOException,InterruptedException
	{
		String str[]=values.toString().split(",");
		float amt =Float.parseFloat(str[3]);
		String month = str[1].substring(0,2);
        context.write(new Text(month),new FloatWritable(amt));
		
		
	}
	} 
	
	public static class CaderPartitioner extends
	   Partitioner < Text, FloatWritable >
	   {
	      @Override
	      public int getPartition(Text key,FloatWritable value, int numReduceTasks)
	      {
	    	String mon=key.toString();
	        int month=Integer.parseInt(mon);
	        
	        if(month==1)
	        {
	        	return 0 % numReduceTasks;
	        }
	        else if(month==2)
	        {
	        	return 1 % numReduceTasks;
	        }
	        else if(month==3)
	        {
	        	return 2 % numReduceTasks;
	        }
	        else if(month==4)
	        {
	        	return 3 % numReduceTasks;
	        }
	        else if(month==5)
	        {
	        	return 4 % numReduceTasks;
	        }
	        else if(month==6)
	        {
	        	return 5 % numReduceTasks;
	        }
	        else if(month==7)
	        {
	        	return 6 % numReduceTasks;
	        }
	        else if(month==8)
	        {
	        	return 7 % numReduceTasks;
	        }
	        else if(month==9)
	        {
	        	return 8 % numReduceTasks;
	        }
	        else if(month==10)
	        {
	        	return 9 % numReduceTasks;
	        }
	        else if(month==11)
	        {
	        	return 10 % numReduceTasks;
	        }
	        else
	        {
	        	return 11 % numReduceTasks;
	        }
	      }
	   }

	
	public static class SepFilesReduce extends Reducer<Text,FloatWritable,Text,FloatWritable>
	{
	public void reduce(Text k,Iterable<FloatWritable> val,Context context) throws IOException,InterruptedException
	{   float sum=0;
		for(FloatWritable f:val)
		{
			sum=sum+f.get();
		}
			
		context.write(k,new FloatWritable (sum));
	}
	}
	
	public static void main(String args[]) throws Exception
	{
	Configuration conf=new Configuration();
	Job job=Job.getInstance(conf,"Amount Group by Month");
	
	job.setJarByClass(SepFiles.class);
	job.setMapperClass(SepFilesMap.class);
	job.setPartitionerClass(CaderPartitioner.class);
	job.setReducerClass(SepFilesReduce.class);
	
	job.setNumReduceTasks(12);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(FloatWritable.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(FloatWritable.class);
	
	FileInputFormat.addInputPath(job,new Path (args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	
	System.exit(job.waitForCompletion(true)? 0 : 1);
	}
	}
  

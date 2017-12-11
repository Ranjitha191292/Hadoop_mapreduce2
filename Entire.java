import java.io.*;
//import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Entire
{
	public static class EntireMap extends Mapper<LongWritable,Text,Text,Text>
	{
	public void map(LongWritable keys,Text values,Context context) throws IOException,InterruptedException
	{
		String str[]=values.toString().split(",");
		//float amt =Float.parseFloat(str[3]);
		String month = str[1].substring(0,2);
		String big=str[0]+" "+str[2]+" "+str[3]+" "+str[4]+" "+str[5]+" "+str[6]+" "+str[7]+" "+str[8];
        context.write(new Text(month),new Text(big));
		
		
	}
	} 
	
	public static class Cader extends
	   Partitioner < Text, Text >
	   {
	      @Override
	      public int getPartition(Text key,Text value, int numReduceTasks)
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

	
	public static class EntireReduce extends Reducer<Text,Text,Text,Text>
	{
	public void reduce(Text k,Iterable<Text> val,Context context) throws IOException,InterruptedException
	{   
		for(Text t:val)
		{			
		context.write(k,t);
		}
	}
	}
	
	public static void main(String args[]) throws Exception
	{
	Configuration conf=new Configuration();
	Job job=Job.getInstance(conf,"Amount Group by Month");
	
	job.setJarByClass(Entire.class);
	job.setMapperClass(EntireMap.class);
	job.setPartitionerClass(Cader.class);
	job.setReducerClass(EntireReduce.class);
	
	job.setNumReduceTasks(12);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	
	FileInputFormat.addInputPath(job,new Path (args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	
	System.exit(job.waitForCompletion(true)? 0 : 1);
	}
	}
  

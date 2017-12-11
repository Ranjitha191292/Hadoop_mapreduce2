import java.io.*;
//import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Logs 
{
	public static class LogsMapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
			//String ent=key.toString();
			String val[]=value.toString().split(" ");
			String logtype=val[3];
			String mod=val[2];
			context.write(new Text(logtype),new Text(mod));
			
		}
	}
	
	public static class LogsCader extends Partitioner <Text, Text>
	   {
		 @Override
	     
		  public int getPartition(Text key,Text value, int numReduceTasks)
	      {
	    	  String type=key.toString();
	    	  if(type.equals("[ERROR]"))
	    	  {
	    		  return 0 % numReduceTasks; 
	    	  }
	    	  else if(type.equals("[DEBUG]"))
	    	  {
	    		  return 1 % numReduceTasks; 
	    	  }
	    	  else if(type.equals("[TRACE]"))
	    	  {
	    		  return 2 % numReduceTasks;
	    	  }
	    	  else
	    	  {
	    		  return 3 % numReduceTasks; 
	    	  }
	    	  
	       }
	   }
	
	public static class LogsReducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text k,Text val,Context context) throws IOException,InterruptedException
		{
				
			context.write(k,val);
			
		}
	}
	
	public static void main(String args[]) throws Exception
	{
	Configuration conf=new Configuration();
	Job job=Job.getInstance(conf,"Log");
	
	job.setJarByClass(Logs.class);
	job.setMapperClass(LogsMapper.class);
	job.setPartitionerClass(LogsCader.class);
	job.setReducerClass(LogsReducer.class);
    
	job.setNumReduceTasks(4);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	
	FileInputFormat.addInputPath(job,new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	
	System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}



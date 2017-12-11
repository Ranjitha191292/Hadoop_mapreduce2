import java.io.*;
//import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Module
{
	public static class ModuleMapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
			
			String val[]=value.toString().split(" ");
			String logtype=val[3];
			String module=val[2];
			context.write(new Text(module),new Text(logtype));
			
		}
	}
	public class ModuleCombiner extends Reducer<Text, Text, Text, IntWritable> {
		public void reduce(Text inpK,Iterable<Text> inpV, Context context) throws IOException, InterruptedException{
	         int sum=0;
			for(Text t:inpV)
	     {
	    	String u=t.toString();
	    	if(u.equals("[ERROR]"));
	    	{
	           sum=sum+1;
	    	}
	     }
        context.write(inpK,new IntWritable(sum));
	}
	}
	
	
	public static class ModuleReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void reduce(Text k,Iterable<IntWritable> val,Context context) throws IOException,InterruptedException
		{
		int s=0;
		int max=0;
		Text key=null;
		
			for(IntWritable t:val)
			{			
			  s=Integer.parseInt(t.toString());
			  if(max<s)
			  {
				  max=s;
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
	
	job.setJarByClass(Module.class);
	job.setMapperClass(ModuleMapper.class);
	job.setReducerClass(ModuleReducer.class);
	job.setCombinerClass(ModuleCombiner.class);
    
	job.setNumReduceTasks(1);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	
	FileInputFormat.addInputPath(job,new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	
	System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}



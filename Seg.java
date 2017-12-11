import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Seg {
	public static class Maps extends Mapper<LongWritable,Text,Text,FloatWritable>
	{
		 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	      {	    	  
	        String str[]=value.toString().split(",");
	        float rtt=Float.parseFloat(str[3]);
	        context.write(new Text(str[0]),new FloatWritable(rtt));
	      }
	}
			 
	public static class Reduces extends Reducer<Text,FloatWritable,Text,FloatWritable>
	   {
		    
		    
		    public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException
		    {
		    	
		    	
		        
		      for(FloatWritable t:values)
		      {
		    	  if(t.get()>160)
		    	  {
		    		  context.write(key,t); 
		    		  
		    	  }
		    	  
		      }
	
		     
		    }
	   }
			 
			 
	 
	  public static void main(String[] args) throws Exception
	  {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "Transaction Count");
		    job.setJarByClass(Seg.class);
		    job.setMapperClass(Maps.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(Reduces.class);
		    job.setNumReduceTasks(1);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(FloatWritable.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(FloatWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

	

}

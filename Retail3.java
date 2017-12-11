
import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Retail3 
{
	
	
	

	
	public static class Mappy extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key,Text values,Context context) throws IOException, InterruptedException
		{
			String str[]=values.toString().split(";");
			String prodid=str[5];
			int sales=Integer.parseInt(str[8]);
			
			
			context.write(new Text(prodid),new IntWritable(sales));
			
			
		}

   
	}
	public static class Reducy extends Reducer<Text,IntWritable,Text,IntWritable>
    {
   	 int sum=0;
   	 float sale=0;
   	 String st=null;
   	 String k=null;
   	 String sum1=null;
   	 int i=0;
   	 Map<Integer,String> abmap = new TreeMap<>(Collections.reverseOrder());
   	 
   	 public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
   		 
   	
   		 
   		 
   		 for(IntWritable t:values)
   			
   		 {
   			sum=sum+t.get();
   			
   	     }
   	     //k=key.toString(); 
   	     //Integer i=sum;
   		 //abmap.put(i,k);
   		 //String proid=abmap.get(i).toString();
   		 //String salsum=abmap.keySet().toString();
   		    
   		 
   		 context.write(key,new IntWritable(sum));
   		 }
   	     

   	 
         }
     
     public static void main(String args[]) throws Exception
     { 
    	Configuration conf=new Configuration() ;
    	Job job = Job.getInstance(conf);
	    job.setJarByClass(Retail3.class);
	    job.setJobName("Reduce Side Join");
	    job.setMapperClass(Mappy.class);
		job.setReducerClass(Reducy.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		
		 
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    	
     }
}





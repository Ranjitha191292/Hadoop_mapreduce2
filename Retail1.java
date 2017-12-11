import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Retail1 {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key,Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(";");
                     String cat_id=str[5]+";"+str[4];
                     String profit=str[8]+";"+str[7];
	             context.write(new Text(cat_id),new Text(profit));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	public static class ReduceClass extends Reducer<Text,Text,Text,IntWritable>
	  {
		    
		    
		  public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
			  int profit=0;
			 //int sum=0;
			  for(Text val:values)
			  {
		       Text arr=val; 
		       String ar=arr.toString();
		       String[] vr=ar.split(";");
		       
		        int tsales=Integer.parseInt(vr[0]);
		        int tcost=Integer.parseInt(vr[1]);
		        profit= tsales-tcost;
		       //sum=sum+profit;
			  }
		    	
			  	
		      context.write(key, new IntWritable(profit));
		    }
	   }
	 
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Gross profit");
		    job.setJarByClass(Retail1.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setNumReduceTasks(1);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
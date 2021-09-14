import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Twitter {
	
	

		  
        public static class MyMapper1 extends Mapper<Object,Text,IntWritable,IntWritable> {
		        @Override
		        public void map ( Object key, Text value, Context context )
		                        throws IOException, InterruptedException {
		                Scanner s = new Scanner(value.toString()).useDelimiter(",");
	                        int id = s.nextInt();
                                int follower_id = s.nextInt();
		            
	
		            context.write(new IntWritable(follower_id),new IntWritable(id));
		            s.close();
      	        }
		    }
	
	

		  
	public static class MyReducer1 extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
                       @Override
                       public void reduce ( IntWritable follower_id, Iterable<IntWritable> ids, Context context )
                           throws IOException, InterruptedException {
            
                            int count = 0;
                            for (IntWritable v: ids) {
                            count++;
                            }   
                       context.write(follower_id,new IntWritable(count));
          }
             }  
		  
	
	
		  
		  
	  public static class MyMapper2 extends Mapper<Object,Text,IntWritable,IntWritable> {
		        @Override
		        public void map ( Object key, Text value, Context context )
		                        throws IOException, InterruptedException {
		            Scanner s = new Scanner(value.toString()).useDelimiter("\t");
	                    int follower_id = s.nextInt();
                            int count = s.nextInt();
		            
		         context.write(new IntWritable(count),new IntWritable(1));
		         s.close();
    	        }
		    }
		  
		  

		
		  
		  
		  public static class MyReducer2 extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
		        @Override
		        public void reduce ( IntWritable count, Iterable<IntWritable> values, Context context )
		                           throws IOException, InterruptedException {
		            
		        	
		            int sum = 0;
		            for (IntWritable v: values) {
		                sum+=v.get();
		            };
		            
		            context.write(count,new IntWritable(sum));
		 }
		    }  	  
	
	
                  public static void main ( String[] args ) throws Exception {
    	
    	               Job job1 = Job.getInstance();
                       job1.setJobName("MyJob1");
                       job1.setJarByClass(Twitter.class);
                       job1.setOutputKeyClass(IntWritable.class);
                       job1.setOutputValueClass(IntWritable.class);
                       job1.setMapOutputKeyClass(IntWritable.class);
                       job1.setMapOutputValueClass(IntWritable.class);
                       job1.setMapperClass(MyMapper1.class);
                       job1.setReducerClass(MyReducer1.class);
                       job1.setInputFormatClass(TextInputFormat.class);
                       job1.setOutputFormatClass(TextOutputFormat.class);
                       FileInputFormat.setInputPaths(job1,new Path(args[0]));
                       FileOutputFormat.setOutputPath(job1,new Path(args[1]));
                       job1.waitForCompletion(true);
        
                       Job job2 = Job.getInstance();
                       job2.setJobName("MyJob2");
                       job2.setJarByClass(Twitter.class);
                       job2.setOutputKeyClass(IntWritable.class);
                       job2.setOutputValueClass(IntWritable.class);
                       job2.setMapOutputKeyClass(IntWritable.class);
                       job2.setMapOutputValueClass(IntWritable.class);
                       job2.setMapperClass(MyMapper2.class);
                       job2.setReducerClass(MyReducer2.class);
                       job2.setInputFormatClass(TextInputFormat.class);
                       job2.setOutputFormatClass(TextOutputFormat.class);
                       FileInputFormat.setInputPaths(job2,new Path(args[1]));
                       FileOutputFormat.setOutputPath(job2,new Path(args[2]));
                       job2.waitForCompletion(true);
             }
                }


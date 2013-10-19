package com.rjkrsinghhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class App 
{
    public static void main( String[] args ) throws Exception
    {
    	if(args.length !=2 ){
    		System.err.println("Usage : MaxTemprature <input path> <output path>");
    		System.exit(-1);
    	}
    	Configuration conf = new Configuration();
    	conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
    	Job job = new Job(conf);
    	job.setJarByClass(App.class);
    	job.setJobName("Patent Citation");
    	
    	FileInputFormat.addInputPath(job,new Path(args[0]) );
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	
    	job.setMapperClass(CiteMapper.class);
    	job.setReducerClass(CiteReducer.class);
    	
    	job.setInputFormatClass(KeyValueTextInputFormat.class);
    	job.setOutputFormatClass(TextOutputFormat.class);
    	
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);
    	
    	
    	
    	System.exit(job.waitForCompletion(true) ? 0:1);
    }
}

package com.rjkrsinghhadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTempratureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {

		int maxVale = Integer.MIN_VALUE;
		for(IntWritable value : values){
			maxVale = Math.max(maxVale, value.get());
		}
		context.write(key,new IntWritable(maxVale));
	}

}

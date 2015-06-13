package com.cgu.superii;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) throws IOException, Exception, Exception {
		long StartTime = System.currentTimeMillis();
		long AverageTime = 0;
		Configuration conf = new Configuration();
		conf.set("mapreduce.jobtracker.address", "local");
		conf.set("fs.defaultFS","file:///");
		Job job = new Job(conf, "xxxx");
		job.setJarByClass(App.class);
		job.setMapperClass(StaticMaper.class);
		job.setCombinerClass(StaticReduer.class);
		job.setReducerClass(StaticReduer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// TODO: specify input and output DIRECTORIES (not files)
		
		FileInputFormat.setInputPaths(job, 
				"/home/jhihzonghu/3YearFile.csv");
		FileOutputFormat.setOutputPath(job, new Path("/home/jhihzonghu/DRGs_Class24/"));
		job.waitForCompletion(true);
		long ProcessTime = System.currentTimeMillis() - StartTime;
		AverageTime += ProcessTime;
		System.out.print(AverageTime/1000);

	}

}

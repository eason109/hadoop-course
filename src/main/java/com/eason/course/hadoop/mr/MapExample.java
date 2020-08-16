package com.eason.course.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapExample extends Configured implements Tool {
	enum Counter {
		LINESKIP,
	}

	public static class MaperExample extends Mapper<LongWritable, Text, NullWritable, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			try {
				String[] lineSplit = line.split("\t");
				String phone = lineSplit[1];
				String macAddr = lineSplit[2];
				Text out = new Text(phone + " " + macAddr);
				context.write(NullWritable.get(), out);

			} catch (ArrayIndexOutOfBoundsException e) {
				context.getCounter(Counter.LINESKIP).increment(1);
				return;
			}
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = new Job(conf, MapExample.class.getSimpleName());
		job.setJarByClass(MapExample.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MaperExample.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.waitForCompletion(true);

		System.out.println("任务名称：" + job.getJobName());
		System.out.println("任务成功：" + (job.isSuccessful() ? "是" : "否"));
		System.out.println("跳过行数：" + job.getCounters().findCounter(Counter.LINESKIP).getValue());
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new MapExample(), args);
	}
}

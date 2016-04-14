package nuk.csie.bigdata.ex1;

import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Ex1Driver {

	public static void main(String[] args) throws Exception {
		
		
		// 建立Job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Add100");
		job.setJarByClass(nuk.csie.bigdata.ex1.Ex1Driver.class);
		// 指派Mapper和Reducer
		job.setMapperClass(nuk.csie.bigdata.ex1.Ex1Map.class);
		job.setReducerClass(nuk.csie.bigdata.ex1.Ex1Reduce.class);

		// 指派輸出型態
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(IntWritable.class);
		
		// 輸入及輸出路徑
		Path input = new Path(Paths.get("data", "ex1.txt").toString());
		Path output = new Path("out", "ex1");
		// 指派Job的輸入及輸出路徑
		FileInputFormat.setInputPaths(job, input);
		FileSystem fs = FileSystem.get(conf);
		fs.delete(output, true);
		FileOutputFormat.setOutputPath(job, output);

		if (!job.waitForCompletion(true))
			return;
	}

}

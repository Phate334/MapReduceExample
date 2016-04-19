package pra2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Pra2Driver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Wrod classer");
		job.setJarByClass(pra2.Pra2Driver.class);
		job.setMapperClass(pra2.Pra2Map.class);

		job.setReducerClass(pra2.Pra2Reduce.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		Path input = new Path("data", "pra1.txt");
		Path output = new Path("out", "pra2");
		FileSystem fs = FileSystem.get(conf);
		fs.delete(output, true);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		if (!job.waitForCompletion(true))
			return;
	}

}

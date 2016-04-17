package nuk.csie.bigdata.ex2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Ex2Driver {

	public static void main(String[] args) throws Exception {

		// 建立Job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "NumberClasser");
		job.setJarByClass(nuk.csie.bigdata.ex2.Ex2Driver.class);

		// 指派Mapper和Reducer
		job.setMapperClass(nuk.csie.bigdata.ex2.Ex2Map.class);
		job.setReducerClass(nuk.csie.bigdata.ex2.Ex2Reduce.class);

		// 指派輸出型態
		job.setMapOutputKeyClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// 輸入及輸出路徑
		Path input = new Path("data", "ex2.txt");
		Path output = new Path("out", "ex2");
		// 指定輸出路徑
		FileInputFormat.setInputPaths(job, input);
		FileSystem fs = FileSystem.get(conf);
		fs.delete(output, true);
		FileOutputFormat.setOutputPath(job, output);
		// 告訴hadoop不要先產生輸出檔，才不會產生原本開頭是part的多餘檔案
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		if (!job.waitForCompletion(true))
			return;
	}

}

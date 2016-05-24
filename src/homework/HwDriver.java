package homework;

import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HwDriver {
	private static String[] tagName = { "instant", "dteday", "season", "yr", "mnth", "hr", "holiday", "weekday",
			"workingday", "weathersit", "temp", "atemp", "hum", "windspeed", "casual", "registered", "cnt" };

	public static void main(String[] args) throws IllegalArgumentException, Exception {
		Configuration conf = new Configuration();
		// 將參數寫入工作的設定檔中
		if(args.length == 0)
		{
			throw new IllegalArgumentException("need less 1 tag name");
		}
		IntWritable[] argArray = new IntWritable[args.length];
		for (int i = 0; i < args.length; i++) {
			// 檢查輸入參數是否在資料欄位中
			int tagIndex = getTagIndex(args[i]);
			if (tagIndex < 0) {
				throw new IllegalArgumentException(args[i]);
			}
			// 記錄欄位順序與欄位名字
			argArray[i] = new IntWritable(tagIndex);
		}
		DefaultStringifier.storeArray(conf, argArray, "args");

		// 開始建立Job
		Job job = Job.getInstance(conf, "BikeShareGroupBy");
		job.setJarByClass(homework.HwDriver.class);
		job.setMapperClass(homework.HwMap.class);
		job.setReducerClass(homework.HwReduce.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		Path outPath = new Path(Paths.get("out", "hw", String.join("_", args)).toString());
		FileInputFormat.setInputPaths(job, new Path("data", "hour.csv"));
		FileOutputFormat.setOutputPath(job, outPath);
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outPath, true);

		if (!job.waitForCompletion(true))
			return;
	}
	
	private static int getTagIndex(String arg)
	{
		for(int i=0; i<tagName.length; i++)
		{
			if(arg.equals(tagName[i]))
			{
				return i;
			}
		}
		return -1;
	}
}

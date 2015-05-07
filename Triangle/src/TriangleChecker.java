import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TriangleChecker {
	
	public static class TriangleCheckerMapper extends Mapper<Object, Text, Text, Text>
	{
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
		
			FileSplit split = (FileSplit)context.getInputSplit();
			String filePath = split.getPath().toString();
			int endIndex = filePath.lastIndexOf("/");
			int startIndex = filePath.lastIndexOf("/", endIndex-1);
			String content = filePath.substring(startIndex+1, endIndex);
			String endStr = content.substring(content.length()-1);
			context.write(value, new Text(endStr));
		}
	}
	
	public static class TriangleCheckerReducer extends Reducer<Text, Text, Text, NullWritable>
	{
		private static long count = 0;

		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			int curCnt = 0;
			boolean existFlag = false;
			
			for(Text curText : values)
			{
				String curVal = curText.toString();
				if(curVal.equals("0"))
				{
					existFlag = true;
				}
				else if(curVal.equals("1"))
				{
					curCnt++;
				}
			}
			if(existFlag)
			{
				count += curCnt;
			}
		}
		
		@Override
		protected void cleanup(
				Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			context.write(new Text(count + ""), NullWritable.get());
			
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "TriangleChecker");
		
		job.setJarByClass(TriangleChecker.class);
		job.setMapperClass(TriangleChecker.TriangleCheckerMapper.class);
		job.setReducerClass(TriangleChecker.TriangleCheckerReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}

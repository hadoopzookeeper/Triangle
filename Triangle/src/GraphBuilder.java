
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GraphBuilder {
	
	public static class GraphBuilderMapper extends Mapper<Object, Text, Text, NullWritable> {

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			String curLine = value.toString();
			String[] idArr = curLine.split(" ");
			Integer id1 = Integer.parseInt(idArr[0]);
			Integer id2 = Integer.parseInt(idArr[1]);
			if(id1 != id2)
			{
				if(id1 > id2)
				{
					context.write(new Text(id2 + "_" + id1), NullWritable.get());
				}
				else
				{
					context.write(new Text(id1 + "_" + id2), NullWritable.get());
				}
			}
		}
		
	}
	
	public static class GraphBuilderCombiner extends Reducer<Text, NullWritable, Text, NullWritable>
	{		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
	
			context.write(key, NullWritable.get());
		}
	}
	
	public static class GraphiBuilderReducer extends Reducer<Text, NullWritable, Text, Text>
	{
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] keys = key.toString().split("_");
			context.write(new Text(keys[0]), new Text(keys[1]));
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "GraphBuilder");
		
		job.setJarByClass(GraphBuilder.class);
		job.setMapperClass(GraphBuilder.GraphBuilderMapper.class);
		job.setCombinerClass(GraphBuilder.GraphBuilderCombiner.class);
		job.setReducerClass(GraphBuilder.GraphiBuilderReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}


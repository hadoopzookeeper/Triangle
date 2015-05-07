import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GraphLinker {
	
	public static class GraphLinkerMapper extends Mapper<Object, Text, Text, Text> 
	{
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
		
			String curLine = value.toString();
			String[] idArr = curLine.split("\t");
			context.write(new Text(idArr[0]), new Text("lt:" + curLine));
			context.write(new Text(idArr[1]), new Text("gt:" + curLine));
		}
	}
	
	public static class GraphLinkerReducer extends Reducer<Text, Text, Text, Text>
	{
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String curStr;
			ArrayList<String> ltValArrList = new ArrayList<String>();
			ArrayList<String> gtValArrList = new ArrayList<String>();
			
			for(Text curText : values)
			{
				curStr = curText.toString();
				String[] splits = curStr.split(":");
				String title = splits[0];
				String[] idArr = splits[1].split("\t");
				if(title.equals("lt"))
				{
					ltValArrList.add(idArr[1]);
				}
				else if(title.equals("gt"))
				{
					gtValArrList.add(idArr[0]);
				}
			}
			
			String curId1, curId2;
			
			for(int i = 0 ; i < ltValArrList.size() ; ++i)
			{
				curId1 = ltValArrList.get(i);
				for(int j = 0 ; j < gtValArrList.size() ; ++j)
				{
					curId2 = gtValArrList.get(j);
					context.write(new Text(curId2), new Text(curId1));
				}
			}
			
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "GraphLinker");
		
		job.setJarByClass(GraphLinker.class);
		job.setMapperClass(GraphLinker.GraphLinkerMapper.class);
		job.setReducerClass(GraphLinker.GraphLinkerReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);  
	}
}

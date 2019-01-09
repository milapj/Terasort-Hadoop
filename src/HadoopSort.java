import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.*;


public class HadoopSort{
	private static Configuration con = new Configuration();
	private static Job work;
	public HadoopSort() throws IOException {
		Job work = new Job(con,"Hadoop sort");
		// TODO Auto-generated constructor stub
	}
	public static class SortingMapper extends Mapper<Object, Text, Text, Text>{


		
		public void map (Object key, Text value, Context context)throws IOException, InterruptedException{
	        
				String value123= value.toString();
	          String kfl = value123.substring(0, 10);
	          String olli=value123;
	          
	          Text keytext = new Text(kfl);
	          Text keyval = new Text(olli);
	          
	          
	          context.write(keytext,keyval);
		}
	}
	public static class SortingReducer extends Reducer<Text, Text, Text, Text>{
		
		private Text outkey = new Text();
		private Text outval = new Text();
		public void reduce (Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
		
		outkey = key;
		for (Text val : values){
			outval = val;
		}
		context.write(outkey,outval);
		}

	}
	public static void config(String[] args) throws IOException
	{
		con.set("mapred.compress.map.output", "true");
		con.set("mapred.output.compression.type", "BLOCK"); 
		con.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		work.setJarByClass(HadoopSort.class);
		work.setMapperClass(SortingMapper.class);
		work.setCombinerClass(SortingReducer.class);
		work.setReducerClass(SortingReducer.class);
		work.setOutputKeyClass(Text.class);
		work.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(work, new Path(args[0]));
		FileOutputFormat.setOutputPath(work, new Path("/tmp/output"));
		
	}
	
	public static void main(String[] args) throws Exception{
		

		long start = System.currentTimeMillis();
		config(args);
		
		
		
		if (work.waitForCompletion(true))
		{
			long end = System.currentTimeMillis();
			
			long Total = end - start;
			System.out.println("Total Elapsed Time on Hadoop: " + Total);			
			System.exit(0);
		}

		else
		{
			long end = System.currentTimeMillis();
			
			
			long Total = end - start;
			System.out.println("Total Elapsed Time on Hadoop: " + Total);
			System.exit(1);	
		}
		

		
	}

}
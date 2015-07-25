import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Flipkart extends Configured{

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		Job job = new Job(conf,"Flipkart");
		job.setJarByClass(Flipkart.class);
		job.setJobName("Flipkart File1 Job");
		
		job.setMapperClass(FlipkartMapper.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(FlipkartReducer.class);

		String pathstart="/home/HadoopUser/Desktop/Dataset/AllFiles/Yes/laptop";
		for(int i=1;i<=50;i++)
		{
			String si=Integer.toString(i);
			String wholepath=pathstart+si;
			FileInputFormat.addInputPath(job, new Path(wholepath));
		}
		FileSystem fs=FileSystem.get(conf);
        Path p=new Path("/home/HadoopUser/DataTypesMod/output-prodinfo");
        fs.delete(p,true);
        
		FileOutputFormat.setOutputPath(job,p);
		
		job.setInputFormatClass(CustomFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}

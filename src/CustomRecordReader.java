import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CustomRecordReader extends RecordReader<LongWritable, Text>{
	private LongWritable key = new LongWritable();
	private Text value = new Text();
	boolean processed = false;
	String data ;
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		FileSplit split = (FileSplit) genericSplit;
		FileSystem fs = FileSystem.get(conf);
		 final Path file = split.getPath();
		FSDataInputStream hfs = fs.open(split.getPath());
		StringBuilder sb = new StringBuilder();
	    try {
	        InputStreamReader r = new InputStreamReader(hfs, "UTF-8");
	        int c = 0;
	        while ((c = r.read()) != -1) {
	            sb.append((char) c);
	        }
	    } catch (IOException e) {
	        throw new RuntimeException(e);
	    }
	  
		data =   sb.toString();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		if(!processed){
			value.set(data);
			processed = true;
			return processed;
		}
		return false;
	}

}
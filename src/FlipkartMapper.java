import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlipkartMapper extends Mapper<LongWritable, Text, Text, NullWritable>
{
   
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException 
	{
	
		
		String html=value.toString();
		boolean found=false;
		
		Pattern patternbr = Pattern.compile("(?<=\'brand\' : \").*(?=\")");
		Matcher matcherbr = patternbr.matcher(html);
	
		Pattern patternfn = Pattern.compile("(?<=\'fn\' : \").*(?=\")");
		Matcher matcherfn = patternfn.matcher(html);
		
		Pattern patternpr = Pattern.compile("(?<=\'price\' : \").*(?=\")");
		Matcher matcherpr = patternpr.matcher(html);
		
		Pattern patternoff = Pattern.compile("(?<=\"discount fk-green\">).*(?=% OFF)");
		Matcher matcheroff = patternoff.matcher(html);
		
		Pattern patternst = Pattern.compile("(?<=stars\" title=\").*(?= stars)");
		Matcher matcherst = patternst.matcher(html);
		
		String s1 = new String();
		String s2 = new String();
		String s3 = new String();
		String s4 = new String();
		String s5 = new String();
		
		while (matcherbr.find())
		{
			s1 = matcherbr.group().toString();
		}	
    
		while (matcherfn.find())
		{
			s2 = matcherfn.group().toString();
		}
    
		while (matcherpr.find())
		{
			s3 = matcherpr.group().toString();
			s3=s3.substring(0, 6);
		}
    
		while (matcheroff.find())
		{
			s4 = matcheroff.group().toString();
			found=true;
		}
		
		
		while (matcherst.find())
		{
			s5 = matcherst.group().toString();
		}
		
		if(found==false)
			s4="0";
		
		if(s2.substring(0,9).equals("HP Compaq"))
			s1="Compaq";
		StringBuffer sb3 = new StringBuffer(s3);
		if(sb3.charAt(2)==',')
			sb3.deleteCharAt(2);
		if(sb3.charAt(3)==',')
			sb3.deleteCharAt(3);
		String output = s1+"\t"+s2+"\t"+sb3+"\t"+s4+"\t"+s5+'\t';
		System.out.println(output+"----");
		context.write(new Text(output),NullWritable.get() );
		
	}
}

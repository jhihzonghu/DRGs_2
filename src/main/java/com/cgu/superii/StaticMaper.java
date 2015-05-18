package com.cgu.superii;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StaticMaper extends Mapper<Object, Text, Text, Text> {
	// private String PID ;
	private int TOTALCOLUM =29 ; 
	private String[] AfterSplit ;
	private String KeyIndex,part2,FeeColum ; 
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// super.map(key, value, context);
		AfterSplit = value.toString().split(",");
		
		if((AfterSplit.length==29)&&!AfterSplit[8].equals("")&&(!AfterSplit[11].equals(""))&&(!AfterSplit[12].equals("")))
		{
			System.out.println(AfterSplit[12]);
			KeyIndex = AfterSplit[8]+"-"+AfterSplit[9].replace("/", "")+AfterSplit[10].replace("/", "");
			ifSameKeyIndex(KeyIndex,AfterSplit);
			if(AfterSplit[11].equals("10"))
			{
				
				if(IfHasSpecilalICDNo(AfterSplit))
				{
				// KeyIndex ; 
				 part2 = "\t"+AfterSplit[11]+"\t"+AfterSplit[12];
				 FeeColum =""+AfterSplit[17]+"\t"+AfterSplit[27];
				 context.write(new Text(part2), new Text(""+1));
				 init();
				}
			}
		}
	
	}
	private boolean IfHasSpecilalICDNo(String[] aftersplit)
	{
		String[] CancerCode = { "162.0", "162.1", "162.2", "162.3", "162.4",
				"162.5", "162.6", "162.8", "162.9" };
		for(int i = 0; i < 8 ; i+=1 )
		{
			for(int j = 0 ; j < CancerCode.length;j+=1)
			{
				if(aftersplit[i].equals(CancerCode[j])) return true;
			}
		}
	  return false;
	}
	private void init(){
	part2 = "";
	FeeColum = "";
	}
    private void ifSameKeyIndex(String keyindex, String[] aftersplit){
 
    }
	private String Regexp(String string) {
		// TODO Auto-generated method stub
		String Parser = "plist=[\\d*,]*;";
		Pattern SoldDataPattern = Pattern.compile(Parser);
		Matcher GoodMatcher = SoldDataPattern.matcher(string);
		if (GoodMatcher.find()) {
			return GoodMatcher.group();
		} else {
			return "";
		}

	}

}

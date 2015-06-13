package com.cgu.superii;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StaticReduer extends Reducer<Text,Text,Text, Text> {
  String sum  ;
  Text sum2 ; 
  int TotalAccount = 0; 
  String[] CancerCode = {  "P067047B", "P067048B", "P067049B", "P067050B",
			"P067051B", "P068049B" };
	@Override
	protected void reduce(Text PID, Iterable<Text> EveryRowPrice,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	//	super.reduce(arg0, arg1, arg2);
		for(Text AccountEveryRow:EveryRowPrice)
		{
			TotalAccount += Integer.valueOf(AccountEveryRow.toString());
					//sum += ","+AccountEveryRow.toString() ; 
		}
		//System.out.println("PID: \t"+ PID + "  TotalPrice: \t"+TotalAccount);
		context.write(new Text(PID), new Text(""+TotalAccount));
		sum2 = new Text(""); 
		TotalAccount = 0 ; 
	}

}

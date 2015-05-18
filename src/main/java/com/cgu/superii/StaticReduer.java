package com.cgu.superii;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StaticReduer extends Reducer<Text,Text,Text, Text> {
  String sum  ; 
  int TotalAccount = 1; 
	@Override
	protected void reduce(Text PID, Iterable<Text> EveryRowPrice,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	//	super.reduce(arg0, arg1, arg2);
		for(Text AccountEveryRow:EveryRowPrice)
		{
		
			TotalAccount +=Integer.valueOf(AccountEveryRow.toString()) ; 
			//sum += ","+AccountEveryRow.toString() ; 
		}
		//System.out.println("PID: \t"+ PID + "TotalPrice: \t"+sum);
		context.write(PID, new Text(""+TotalAccount));
		sum = "" ; 
		TotalAccount = 0 ; 
	}

}

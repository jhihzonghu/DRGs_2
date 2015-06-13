package com.cgu.superii;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Class24Editon extends Mapper<Object, Text, Text, Text> {

	@Override
	protected void setup(Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}

	private int TOTALCOLUM = 29;
	private String[] AfterSplit;
	private String KeyIndex, part2, FeeColum;

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// super.map(key, value, context);
		AfterSplit = value.toString().split(",");

		if ((AfterSplit.length == 29) && !AfterSplit[8].equals("")
				&& (!AfterSplit[11].equals("")) && (!AfterSplit[12].equals(""))) {
			System.out.println(AfterSplit[12]);
			KeyIndex = AfterSplit[8] + "|" + AfterSplit[9].replace("/", "")
					+ AfterSplit[10].replace("/", "");
			ifSameKeyIndex(KeyIndex, AfterSplit);
			if (AfterSplit[11].equals("10") || AfterSplit[11].equals("12")
					|| AfterSplit[11].equals("24")
					|| AfterSplit[11].equals("9") || AfterSplit[11].equals("3")) {

				if (IfHasSpecilalICDNo(AfterSplit)) {
					// KeyIndex ;

					part2 = "\t" + AfterSplit[11] + "\t" + AfterSplit[12];
					FeeColum = "" + AfterSplit[17] + "\t" + AfterSplit[27];
					context.write(new Text(KeyIndex + part2), new Text(
							AfterSplit[27] + "\t" + 1));
					init();
				} else {
					part2 = "\t" + AfterSplit[11] + "\t" + AfterSplit[12];
					FeeColum = "" + AfterSplit[17] + "\t" + AfterSplit[27];
					context.write(new Text(KeyIndex + part2), new Text("" + 1));
					init();
				}
			}
		}

	}

	private boolean IsSpecialNo(String S) {
		return true;
	}

	private boolean IfHasSpecilalICDNo(String[] aftersplit) {
		String[] CancerCode = { "P067047B", "P067048B", "P067049B", "P067050B",
				"P067051B", "P068049B" };
		for (int i = 0; i < 8; i += 1) {
			for (int j = 0; j < CancerCode.length; j += 1) {
				if (aftersplit[12].equals(CancerCode[j]))
					return true;
			}
		}
		return false;
	}

	private void init() {
		part2 = "";
		FeeColum = "";
	}

	private void ifSameKeyIndex(String keyindex, String[] aftersplit) {

	}
}

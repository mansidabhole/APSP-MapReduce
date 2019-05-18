import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.common.io.Files;

public class APSP {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		//Functions to convert intended MapReduce keys and values to strings.
		public String IJtoString(int i, int j) {
			return new String(String.valueOf(i) + "," + String.valueOf(j));
		}

		public String GXYtoString(int i, int j, int k, String d) {
			StringBuffer sb = new StringBuffer();
			sb.append(i).append(",").append(j).append(":").append(k).append(":").append(d);
			return sb.toString();
		}
		
		// Map Job
		//input of the form i,j:k:distance
		//First time is X,Y. Rest of the times input is X:Y:i:j:absMin:NextT
		//Emitted key is i,j : Value is node1,node2,k,distance
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Text gxy = new Text();
			Text ij = new Text();
			gxy.clear();
			ij.clear();
			String line = value.toString();
			String[] w = line.split(":");
			String[] IJ = w[0].split(",");
			int i = Integer.parseInt(IJ[0]);
			int j = Integer.parseInt(IJ[1]);
			int k = Integer.parseInt(w[1]);
			//int d = Integer.parseInt(w[2]);
			if (i==k || j==k){
				for (int l=1; l<=4; l++){
					if (j==k){
						ij.set(IJtoString(i, l));
						gxy.set(GXYtoString(i, j, k, w[2]));
					    System.out.println("Mapper emitting"+" "+ij+" "+gxy);
						context.write(ij, gxy);
					}
					if (i==k){
						ij.set(IJtoString(l, j));
						gxy.set(GXYtoString(i, j, k, w[2]));
						System.out.println("Mapper emitting"+" "+ij+" "+gxy);
						context.write(ij, gxy);
					}
				}
			}
			else{
				ij.set(IJtoString(i, j));
				gxy.set(GXYtoString(i, j, k, w[2]));
				System.out.println("Mapper emitting"+" "+ij+" "+gxy);
				context.write(ij, gxy);
			}

		}
	} 

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			Boolean flag1;
			Boolean flag2;
			flag1 = false;
			flag2 = false;
			int min = 0;
			int k = 0;
			int val1= Integer.MAX_VALUE;
			int val2= Integer.MAX_VALUE;
			String[] IJ = key.toString().split(",");
			int I = Integer.parseInt(IJ[0]);
			int J = Integer.parseInt(IJ[1]);
			String str = new String();

			for (Text val : values) {
				str = val.toString();
				String[] GXY = str.split(":");   //splitting i,j:k:d
				k = Integer.parseInt(GXY[1]);
				String[] ij = GXY[0].split(",");				// ij = i,j
				int i = Integer.parseInt(ij[0]);
				int j = Integer.parseInt(ij[1]);
				if (i==I && j==J){
					if (GXY[2].equals("*"))
						min = Integer.MAX_VALUE;
					else{
						min = Integer.parseInt(GXY[2]);
						flag1 = true;
					}
				}
				else{
						if (val1==Integer.MAX_VALUE){
							if (!(GXY[2].equals("*")))
								val1 = Integer.parseInt(GXY[2]);
						}
						else{
							if (!(GXY[2].equals("*"))){
								val2 = Integer.parseInt(GXY[2]);
								flag2 = true;
							}
						}
				}
			}
				System.out.println(" value 1 and value 2 addition is "+(val1+val2));
				if (flag2){
					if (min > (val1+val2)){
						min = val1+val2;
						flag1 = true;
					}
				}
				System.out.println(" MIn value is "+min);

			Text pair;
			k=k+1;
			if (flag1)
				pair = new Text(I + "," + J + ":" + k + ":" + min);
			else
				pair = new Text(I + "," + J + ":" + k + ":" + "*");
			System.out.println("Reducer emitting"+pair+" ");
			context.write(pair, null);
		}
	}

	public static void main(String[] args) throws Exception {
		Path outputDir = new Path(args[1]);


		int n = 4;
		int i = 1;

		while(i<=n){
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://localhost:9000");
			FileSystem filesystem = FileSystem.get(conf);	
			Job job = new Job(conf, "GAP");

			filesystem.delete(outputDir, true);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.waitForCompletion(true);

			FileUtil.copy(filesystem, new Path(args[1]+"/"+"part-r-00000"), filesystem, new Path(args[0]+"/"+"file01"), false, conf);
			filesystem.delete(outputDir, true);
			i=i+1;
		} 
	}       
}
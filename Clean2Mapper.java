import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;  
import java.util.Scanner;

import javax.naming.Context;  
public class Clean2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Scanner sc = new Scanner(value.toString());  
        sc.useDelimiter(",");
        String arr[] = new String[789];
        int count = 0;
        while (sc.hasNext()) {  
            arr[count] = sc.next();
            count++;
        }  

        // binary columns for low/med/high temp based on HDD65 and significant/insignificant energy consumption
        // text format iecc climate to not include letter zone

        String str = arr[7];
        str = str.replaceAll("[^0-9]", ""); // text formatting: do not include letter code for IECC

        if (!arr[0].equals("DOEID")){
            context.write(new Text("BTU" + Integer.parseInt(arr[0])), new IntWritable((int)(Float.parseFloat(arr[778]))));
            context.write(new Text("HDD65" + Integer.parseInt(arr[0])), new IntWritable((int)(Float.parseFloat(arr[9]))));
            context.write(new Text("IECC" + Integer.parseInt(arr[0])), new IntWritable(Integer.parseInt(str)));
        }
    }
}

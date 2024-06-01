import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;  
import java.util.Scanner;

import javax.naming.Context;  
public class UATStatsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
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
        if (arr[0].equals("118496")) {
            context.write(new Text(arr[3] + " " + arr[8] + "L"), new IntWritable((int)(Float.parseFloat(arr[778]))));
        } else if (!arr[0].equals("DOEID")) {
            context.write(new Text(arr[3] + " " + arr[8]), new IntWritable((int)(Float.parseFloat(arr[778]))));
        }
    }
}

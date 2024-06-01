import java.io.IOException;
import java.util.*;

import javax.naming.Context;

import java.lang.Math;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class Clean2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        
        for (IntWritable value : values) {
            int cur = value.get();
            if (key.toString().charAt(0) == 'B') {
                if (cur > 27000) { // binary to categorize as high energy consumption
                    context.write(key, new IntWritable(1));
                } else {
                    context.write(key, new IntWritable(0));
                }
            } else if (key.toString().charAt(0) == 'H') {
                if (cur > 4853) { // categorize HDD65 as 2 (hot) 1 or 0 (cold)
                    context.write(key, new IntWritable(2));
                } else if (cur > 4271) {
                    context.write(key, new IntWritable(1));
                } else {
                    context.write(key, new IntWritable(0));
                }
            } else { // IECC letter code (done in mapper)
                context.write(key, new IntWritable(cur));
            }
        }

    }
}

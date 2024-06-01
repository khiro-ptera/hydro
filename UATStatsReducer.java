
import java.io.IOException;
import java.util.Collections;   
import java.util.ArrayList;
import java.lang.Math;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class UATStatsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        // initializing required vars
        int sumU = 0; 
        int numU = 0; 
        int sumC = 0; 
        int numC = 0; 
        int sumR = 0; 
        int numR = 0; 
        // number of rural households for each state
        int total = 0;
        
        for (IntWritable value : values) {

            int cur = value.get();
            int fips = Integer.parseInt(key.toString().substring(0, 2));
            // context.write(new Text("Test"), new IntWritable(fips));
            char UAT = key.toString().charAt(3);
            total++;
            // context.write(new Text(Character.toString(UAT)), new IntWritable(total));
            if (UAT == 'U') {
                numU++;
                sumU += cur;
            } else if (UAT == 'C') {
                numC++;
                sumC += cur;
            } else if (UAT == 'R') {
                numR++;
                sumR += cur;
            }
            
            /*if (total == 100) { // 18496
                if (numU != 0) {
                    context.write(new Text("Mean Urban"), new IntWritable((int)(sumU/numU)));
                }
                if (numC != 0) {
                    context.write(new Text("Mean Cluster"), new IntWritable((int)(sumC/numC)));
                }
                if (numR != 0) {
                    context.write(new Text("Mean Rural"), new IntWritable((int)(sumR/numR)));
                }
            }*/
        }
        
        if (numU != 0) {
            context.write(new Text("Mean Urban, "), new IntWritable((int)(sumU/numU)));
        }
        if (numC != 0) {
            context.write(new Text("Mean Cluster, "), new IntWritable((int)(sumC/numC)));
        }
        if (numR != 0) {
            context.write(new Text("Mean Rural, "), new IntWritable((int)(sumR/numR)));
        }

    }
}


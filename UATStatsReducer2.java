import java.io.IOException;
import java.util.Collections;   
import java.util.ArrayList;
import java.lang.Math;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class UATStatsReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        // initializing required vars
        int [] sumU = new int[56]; 
        // sum of urban consumption for each state
        int [] sumC = new int[56]; 
        // sum of cluster consumption for each state
        int [] sumR = new int[56]; 
        // sum of rural consumption for each state
        int [] numU = new int[56]; 
        // number of urban households for each state
        int [] numC = new int[56]; 
        // number of cluster households for each state
        int [] numR = new int[56]; 
        // number of rural households for each state
        int total = 0;
        
        for (IntWritable value : values) {

            int cur = value.get();
            int fips = Integer.parseInt(key.toString().substring(0, 2));
            // context.write(new Text("Test"), new IntWritable(fips));
            char UAT = key.toString().charAt(3);
            total++;
            if ((fips <= 56) && (fips > 0)) {
                // context.write(new Text(Character.toString(UAT)), new IntWritable(total));
                if (UAT == 'U') {
                    numU[fips-1]++;
                    sumU[fips-1] += cur;
                } else if (UAT == 'C') {
                    numC[fips-1]++;
                    sumC[fips-1] += cur;
                } else if (UAT == 'R') {
                    numR[fips-1]++;
                    sumR[fips-1] += cur;
                }
            }
            
            if (total == 50) { // 18496
                for (int i = 0; i < 56; i++) {
                    if (numU[i] != 0) {
                        context.write(new Text("State" + (i+1) + " Mean Urban"), new IntWritable((int)(sumU[i]/numU[i])));
                    }
                    if (numC[i] != 0) {
                        context.write(new Text("State" + (i+1) + " Mean Cluster"), new IntWritable((int)(sumC[i]/numC[i])));
                    }
                    if (numR[i] != 0) {
                        context.write(new Text("State" + (i+1) + " Mean Rural"), new IntWritable((int)(sumR[i]/numR[i])));
                    }
                }
            }
        }

    }
}

import java.io.IOException;
import java.util.Collections;   
import java.util.ArrayList;
import java.lang.Math;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class StatsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        // initializing required vars
        ArrayList<Integer> vals1 = new ArrayList<Integer>();
        ArrayList<Integer> vals2 = new ArrayList<Integer>();
        ArrayList<Integer> vals3 = new ArrayList<Integer>();
        int sum1 = 0;
        int sum2 = 0;
        int sum3 = 0;

        for (IntWritable value : values) {

            int cur = value.get();
            if (key.toString().charAt(0) == 'B') { // column 1: BTU/hr
                sum1 = sum1 + cur;
                vals1.add(cur);

                if (vals1.size() == 18496) {
                    int mean1 = sum1/vals1.size(); // mean of column 1

                    int nvar = 0; 
                    for(int i = 0; i < vals1.size(); i++) {
                        nvar = nvar + (vals1.get(i)-mean1)*(vals1.get(i)-mean1);
                    }
                    int std1 = (int) Math.sqrt(nvar/vals1.size()); // standard deviation of column 1

                    Collections.sort(vals1);
                    int med1 = (int) vals1.get((int)(vals1.size()/2)); // median of column 1

                    context.write(new Text("Mean BTU/hr"), new IntWritable((int) mean1));
                    context.write(new Text("Stdev BTU/hr"), new IntWritable((int) std1));
                    context.write(new Text("Med BTU/hr"), new IntWritable((int) med1));
                }

            } else if (key.toString().charAt(0) == '$') { // column 2: $/hr
                sum2 = sum2 + cur;
                vals2.add(cur);

                if (vals2.size() == 18496) {
                    int mean2 = sum2/vals2.size(); // mean of column 2

                    int nvar = 0; 
                    for(int i = 0; i < vals2.size(); i++) {
                        nvar = nvar + (vals2.get(i)-mean2)*(vals2.get(i)-mean2);
                    }
                    int std2 = (int) Math.sqrt(nvar/vals2.size()); // standard deviation of column 2

                    Collections.sort(vals2);
                    int med2 = (int) vals2.get((int)(vals2.size()/2)); // median of column 2

                    context.write(new Text("Mean $/hr"), new IntWritable((int) mean2));
                    context.write(new Text("Stdev $/hr"), new IntWritable((int) std2));
                    context.write(new Text("Med $/hr"), new IntWritable((int) med2));
                }

            } else if (key.toString().charAt(0) == 'H') { // column 3: HDD65, a measure of climate
                // this column is the only data to have a significant mode
                sum3 = sum3 + cur;
                vals3.add(cur);

                if (vals3.size() == 18496) {
                    int mean3 = sum3/vals3.size(); // mean of column 3

                    int nvar = 0; 
                    for(int i = 0; i < vals3.size(); i++) {
                        nvar = nvar + (vals3.get(i)-mean3)*(vals3.get(i)-mean3);
                    }
                    int std3 = (int) Math.sqrt(nvar/vals3.size()); // standard deviation of column 3

                    Collections.sort(vals3);
                    int med3 = (int) vals3.get((int)(vals3.size()/2)); // median of column 3

                    int mode3 = -1; // calculate mode of HDD65 (column 3)
                    int most = 0;
                    for(int i = 0; i < vals3.size(); i++) {
                        int c = 0;
                        for(int j = 0; j < vals3.size(); j++) {
                            if (vals3.get(i) == vals3.get(j)) {
                                c++;
                            }
                        }
                        if (c > mode3) {
                            mode3 = vals3.get(i);
                        }
                    }

                    context.write(new Text("Mean HDD65"), new IntWritable((int) mean3));
                    context.write(new Text("Stdev HDD65"), new IntWritable((int) std3));
                    context.write(new Text("Med HDD65"), new IntWritable((int) med3));
                    context.write(new Text("Mode HDD65"), new IntWritable(mode3));
                }
            }
        }
    }
}

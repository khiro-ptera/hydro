import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class CleanReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        ArrayList<Integer> dupe = new ArrayList<Integer>();
        for (IntWritable value : values) {
            int cur = value.get();
            if (!dupe.contains(cur)) {
                if (cur > 0) {
                    context.write(key, new IntWritable(cur));
                }
                dupe.add(cur);
            }
        }
    }
}

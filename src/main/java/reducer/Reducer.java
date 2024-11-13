package reducer;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, FloatWritable, Text, FloatWritable> {
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        // write your code
        float minValue = Float.MAX_VALUE;
        for(FloatWritable value:values){
            minValue = Math.min(minValue,value.get());
        }
        context.write(key,new FloatWritable(minValue));
    }
}
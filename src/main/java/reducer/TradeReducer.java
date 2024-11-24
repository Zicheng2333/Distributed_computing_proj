package reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TradeReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    private Map<String, String> orderDataMap = new HashMap<>();
    @Override
    protected void setup(Context context) throws IOException {
        Path[] cacheFiles = context.getLocalCacheFiles();
        if (cacheFiles != null) {
            for (Path cacheFile : cacheFiles) {
                try (BufferedReader reader = new BufferedReader(new FileReader(cacheFile.toString()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] fields = line.split("\t");
                        orderDataMap.put(fields[0], fields[1]);  //TODO 这里需要保证applseqnum没有重复的！
                    }
                }
            }
        }
    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] fields = value.toString().split("\t");
            if (fields.length == 4) {
                String bidTime = orderDataMap.get(fields[0]);
                String offerTime = orderDataMap.get(fields[1]);

                if (bidTime != null && offerTime != null) {
                    int type = Long.parseLong(bidTime) > Long.parseLong(offerTime) ? 1 : 0;
                    context.write(NullWritable.get(), new Text(value + "\t" + type));
                }
            }
        }
    }
}
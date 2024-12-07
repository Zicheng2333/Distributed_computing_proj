package mapper;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TradeTimeMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
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
                        orderDataMap.put(fields[0], fields[1]); // Key: BidApplSeqNum, Value: transactTime
                    }
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
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
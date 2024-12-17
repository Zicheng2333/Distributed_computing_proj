package mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String securityId; // 邮票代码
    private final String[] fields = new String[20]; // 预分配数组

    protected void setup(Context context) {
        securityId = context.getConfiguration().get("securityID");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        int start = 0, fieldIndex = 0;

        for (int i = 0; i < line.length() && fieldIndex < 20; i++) {
            if (line.charAt(i) == '\t') {
                fields[fieldIndex++] = line.substring(start, i);
                start = i + 1;
            }
        }
        if (fieldIndex < 20) {
            fields[fieldIndex] = line.substring(start);
        }

        if (fields.length == 20 && fields[8].equals(securityId)) {
            context.write(new Text(fields[7]), new Text(fields[12]));
        }
    }
}

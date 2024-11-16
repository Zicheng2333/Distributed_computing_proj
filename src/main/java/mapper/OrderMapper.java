package mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
public class OrderMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String securityId;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        securityId = context.getConfiguration().get("securityID");
        if (fields.length == 20) { // 检查字段数量是否满足要求
            String applSeqNum = fields[7];
            String transactTime = fields[12];
            String sID = fields[8];
            if (sID.equals(securityId)){
                context.write(new Text(applSeqNum), new Text(transactTime));
            }
        }
    }
}
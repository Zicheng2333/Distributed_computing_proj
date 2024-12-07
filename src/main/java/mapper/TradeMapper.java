package mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TradeMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 现在value就是已经经过过滤的数据行
        String[] fields = value.toString().split("\t");
        if (fields.length ==16) {
            // 原逻辑提取出 bidApplSeqNum, offerApplSeqNum, price, tradeQty
            String bidApplSeqNum = fields[10];
            String offerApplSeqNum = fields[11];
            String price = fields[12];
            String tradeQty = fields[13];

            String interValue = bidApplSeqNum+"\t"+offerApplSeqNum+"\t"+price+"\t"+tradeQty;
            context.write(NullWritable.get(), new Text(interValue));
        }
    }
}
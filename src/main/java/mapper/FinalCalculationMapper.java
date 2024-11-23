package mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FinalCalculationMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length != 5) {
            return; // 跳过格式不正确的数据
        }

        String timeStart = context.getConfiguration().get("timeStart");
        String timeEnd = context.getConfiguration().get("timeEnd");
        String timeWindow = timeStart + "-" + timeEnd; // 时间窗口
        long tradeQty = Long.parseLong(fields[1]); // 成交量
        double tradeAmount = Double.parseDouble(fields[2]); // 成交额
        String label = fields[3]; // 分类标签 (超大、大、中、小)
        String tradeType = fields[4]; // 交易类型 (买/卖)

        // 输出 Key: 时间窗口
        // 输出 Value: 分类标签 + 交易类型 + 成交量 + 成交额
        context.write(new Text(timeWindow), new Text(label + "\t" + tradeType + "\t" + tradeQty + "\t" + tradeAmount));
    }
}

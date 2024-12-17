package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FinalAggregationMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 输入格式: timeWindow \t 原始记录(16字段)
        // 实际上PreprocessJob输出时key=value是timeWindow，val=原记录，中间有\t分隔
        // 假设Preprocess输出时使用 default输出格式(TextOutputFormat),那么输入行大致是:
        // timeWindow <tab> 原始Trade行
        String line = value.toString(); // 将输入行转换为字符串
        int idx = line.indexOf('\t'); // 查找制表符的位置
        if (idx == -1) {
            return; // 格式异常
        }
        String timeWindow = line.substring(0, idx); // 获取timeWindow
        String record = line.substring(idx+1); // 获取原始记录
        context.write(new Text(timeWindow), new Text(record)); // 输出timeWindow和原始记录
    }
}

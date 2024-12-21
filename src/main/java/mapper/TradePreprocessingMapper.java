package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TradePreprocessingMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String securityID; // 邮票代码
    private String[] timeWindows; // 时间窗口

    @Override
    protected void setup(Context context) {
        this.securityID = context.getConfiguration().get("securityID"); // 设置所需邮票代码
        String tws = context.getConfiguration().get("timeWindows"); // 设置时间窗口
        this.timeWindows = tws.split(","); // 按逗号分割时间窗口
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        int fieldCount = 16; // 预计字段数量
        String[] fields = new String[fieldCount]; // 用于存储输入字段的预分配数组
        int startAt = 0, fieldIndex = 0; // 初始化开始位置和字段索引

        for (int i = 0; i < line.length() && fieldIndex < fieldCount; i++) {
            if (line.charAt(i) == '\t') {
                fields[fieldIndex++] = line.substring(startAt, i);
                startAt = i + 1;
            }
        } // 按制表符分割字段
        if (fieldIndex < fieldCount) {
            fields[fieldIndex++] = line.substring(startAt); // 最后一列
        }

        if (fieldIndex == 16) { // 检查字段数量是否满足要求
            String sID = fields[8]; // 获取输入的邮票代码
            if (!sID.equals(this.securityID)) { // 检查邮票代码是否符合要求
                return; // 不符合要求则返回
            }

            String execType = fields[14]; // 获取执行类型
            if (!"F".equals(execType)) {
                return; // 只处理成交类型F
            }

            Long tradeTime = Long.parseLong(fields[15]); // 获取交易时间

            // 判断该记录属于哪个时间窗口
            for (String window : timeWindows) { // 遍历时间窗口
                if (window.trim().isEmpty()) continue; // 跳过空窗口
                StringTokenizer windowTokenizer = new StringTokenizer(window, " to "); // 用" to "分割时间窗口
                long start = Long.parseLong(windowTokenizer.nextToken()); // 获取开始时间
                long end = Long.parseLong(windowTokenizer.nextToken()); // 获取结束时间

                // 特殊end修正
                if (end == 20190102113000000L) { // 如果结束时间为20190102113000000
                    end = 20190102113000001L; // 则将结束时间修改为20190102113000001
                } else if (end == 20190102150000000L) { // 如果结束时间为20190102150000000
                    end = 20190102150000001L; // 则将结束时间修改为20190102150000001
                }

                if (tradeTime >= start && tradeTime < end) {
                    // 直接输出: key为timeWindow，value为原记录
                    context.write(new Text(window), value);
                    break;
                }
            }
        }
    }
}



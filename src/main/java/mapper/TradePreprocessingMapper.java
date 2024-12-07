package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class TradePreprocessingMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String securityID;
    private String[] timeWindows;
    private MultipleOutputs<Text, Text> mos;

    @Override
    protected void setup(Context context) {
        this.securityID = context.getConfiguration().get("securityID");
        String tws = context.getConfiguration().get("timeWindows");
        this.timeWindows = tws.split(",");
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 原始trade数据解析
        String[] fields = value.toString().split("\t");
        // 确保字段数量足够，且能提取securityID和时间字段
        if (fields.length ==16 ) {
            String sID = fields[8];
            if (!sID.equals(this.securityID)) {
                return;
            }

            String execType = fields[14];
            if (!"F".equals(execType)) {
                return; // 只处理成交类型F
            }

            Long tradeTime = Long.parseLong(fields[15]);

            // 判断该记录属于哪个时间窗口
            // timeWindow格式 "startTime-endTime"
            // 找到 tradeTime 满足 start <= tradeTime < end 的窗口
            for (String window : timeWindows) {
                if (window.trim().isEmpty()) continue;
                String[] times = window.split("-");
                long start = Long.parseLong(times[0]);
                long end = Long.parseLong(times[1]);

                // 处理特殊end时间点的修正逻辑
                String windowEnd = times[1];
                if(windowEnd.equals("20190102113000000")){
                    end = 20190102113000001L;
                } else if(windowEnd.equals("20190102150000000")){
                    end = 20190102150000001L;
                }

                if (tradeTime >= start && tradeTime < end) {
                    // 将该记录输出到对应的窗口目录下
                    // 格式化输出目录名称，如 window_YYYYMMDDHHMMSS_YYYYMMDDHHMMSS
                    String outputName = "window_" + window.replace("-", "_");
                    // 将整条记录原样输出（后续Job2的Mapper再做细分处理）
                    mos.write(new Text(value), null, outputName + "/part");
                    break;
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (mos != null) {
            mos.close();
        }
    }
}

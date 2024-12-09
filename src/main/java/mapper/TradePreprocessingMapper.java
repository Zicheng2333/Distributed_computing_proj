package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TradePreprocessingMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String securityID;
    private String[] timeWindows;

    @Override
    protected void setup(Context context) {
        this.securityID = context.getConfiguration().get("securityID");
        String tws = context.getConfiguration().get("timeWindows");
        this.timeWindows = tws.split(",");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 原始trade数据解析
        String[] fields = value.toString().split("\t");
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
            for (String window : timeWindows) {
                if (window.trim().isEmpty()) continue;
                String[] times = window.split("-");
                long start = Long.parseLong(times[0]);
                long end = Long.parseLong(times[1]);

                // 特殊end修正
                if(times[1].equals("20190102113000000")){
                    end = 20190102113000001L;
                } else if(times[1].equals("20190102150000000")){
                    end = 20190102150000001L;
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
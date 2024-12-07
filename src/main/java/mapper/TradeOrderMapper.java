package mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TradeOrderMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private MultipleOutputs<NullWritable, Text> multipleOutputs;
    private String[] timeWindows;
    private String securityID;

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs<>(context);
        String timeWindowParam = context.getConfiguration().get("timeWindows");
        timeWindows = timeWindowParam.split(",");
        securityID = context.getConfiguration().get("securityID");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length == 16) {
            String bidApplSeqNum = fields[10];
            String offerApplSeqNum = fields[11];
            String execType = fields[14];
            String sID = fields[8];
            String trade_Time = fields[15];
            String price = fields[12];
            String tradeQty = fields[13];

            if ("F".equals(execType) && securityID.equals(sID)) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
                    Date tradeTime = sdf.parse(trade_Time);

                    for (String timeWindow : timeWindows) {
                        String[] bounds = timeWindow.split("-");
                        Date startTime = sdf.parse(bounds[0]);
                        Date endTime = sdf.parse(bounds[1]);

                        if (!tradeTime.before(startTime) && !tradeTime.after(endTime)) { //TODO 这里需要处理区间开闭的问题！！！
                            String interValue = timeWindow + "\t" + bidApplSeqNum + "\t" + offerApplSeqNum + "\t" + price + "\t" + tradeQty;
                            multipleOutputs.write("timeWindowOutput", NullWritable.get(), new Text(interValue), timeWindow.replace(":", "_"));
                            break;
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error processing record: " + value + " due to " + e.getMessage());
                }
            }
        } else if (fields.length == 20) {
            if(fields[8].equals(securityID)){
                String applSeqNum = fields[7];
                String transactTime = fields[12];
                multipleOutputs.write("orderOutput", new Text(applSeqNum), new Text(transactTime));
            }
        } else {
            System.err.println("Invalid data format: " + value);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
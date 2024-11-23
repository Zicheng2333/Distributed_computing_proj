package mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TradeMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private String securityID;
    private long timeS;
    private long timeE;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length ==16 ) { // 确保 BidApplSeqNum 和 OfferApplSeqNum 存在
            String bidApplSeqNum = fields[10];
            String offerApplSeqNum = fields[11];
            String execType = fields[14];
            String sID = fields[8];
            Long tradeTime = Long.parseLong(fields[15]);
            String price = fields[12];
            String tradeQty = fields[13];

            timeS = Long.parseLong(context.getConfiguration().get("timeStart"));
            timeE = Long.parseLong(context.getConfiguration().get("timeEnd"));
            securityID = context.getConfiguration().get("securityID");

            String interValue = bidApplSeqNum+"\t"+offerApplSeqNum+"\t"+price+"\t"+tradeQty;

            if("F".equals(execType) && sID.equals(securityID) && tradeTime>=timeS && tradeTime<timeE){
                context.write(NullWritable.get(), new Text(interValue));
            }
        }
    }
}
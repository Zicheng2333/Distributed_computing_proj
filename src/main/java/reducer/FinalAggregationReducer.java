package reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class FinalAggregationReducer extends Reducer<Text, Text, Text, Text> {

    private HashMap<String,String> orderMap = new HashMap<>();
    private long circulatingStock;

    // 最终聚合结果
    private double mainInflow = 0;
    private double mainOutflow = 0;

    private long ultraBuyQty = 0, ultraSellQty = 0;
    private double ultraBuyAmount = 0, ultraSellAmount = 0;
    private long largeBuyQty = 0, largeSellQty = 0;
    private double largeBuyAmount = 0, largeSellAmount = 0;
    private long midBuyQty = 0, midSellQty = 0;
    private double midBuyAmount = 0, midSellAmount = 0;
    private long smallBuyQty = 0, smallSellQty = 0;
    private double smallBuyAmount = 0, smallSellAmount = 0;

    @Override
    protected void setup(Context context) throws IOException {
        this.circulatingStock = Long.parseLong(context.getConfiguration().get("circulatingStock"));
        // 从DistributedCache中加载order数据
        Path[] cacheFiles = context.getLocalCacheFiles();
        if (cacheFiles != null) {
            for (Path cacheFile : cacheFiles) {
                try (BufferedReader reader = new BufferedReader(new FileReader(cacheFile.toString()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] fields = line.split("\t");
                        if (fields.length == 2) {
                            orderMap.put(fields[0], fields[1]); // applSeqNum -> transactTime
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // key: timeWindow
        // values: 若干条原始Trade记录（16字段）
        // 这里的过程：
        // 1. 对该timeWindow的所有记录执行类似Job2 (TradeReducer)逻辑：对每条Trade记录，根据bidApplSeqNum/offerApplSeqNum的时间判断type(买或卖)
        // 2. 对结果再聚合并打标签，与SumThenLabelReducer逻辑相同

        // 为了实现类似原逻辑，需要记录每一个主动单最终汇总： ApplSeqNum, totalQty, totalAmount, tradeType
        // 然后根据circulatingStock对最终成交分级打标签并汇总

        // 临时保存映射 (applSeqNum, [totalQty, totalAmount, type])，其中type为0或1
        HashMap<String, AggData> aggMap = new HashMap<>();

        // 先清理之前窗口的累计（多个timeWindow会多次调用reduce，这里对每个timeWindow重置）
        resetAggregation();

        for (Text val : values) {
            String[] fields = val.toString().split("\t");
            if (fields.length ==16) {
                String bidApplSeqNum = fields[10];
                String offerApplSeqNum = fields[11];
                double price = Double.parseDouble(fields[12]);
                long tradeQty = Long.parseLong(fields[13]);

                String bidTime = orderMap.get(bidApplSeqNum);
                String offerTime = orderMap.get(offerApplSeqNum);
                if (bidTime == null || offerTime == null) {
                    // 找不到对应订单时间，跳过或继续处理
                    continue;
                }
                int type = Long.parseLong(bidTime) > Long.parseLong(offerTime) ? 1 : 0; // 1买，0卖

                // 根据type确定使用的applSeqNum作key：如果type=1（买），主动方是bid方；如果type=0（卖），主动方是offer方
                String activeApplSeqNum = (type == 1) ? bidApplSeqNum : offerApplSeqNum;

                AggData ad = aggMap.getOrDefault(activeApplSeqNum, new AggData(type));
                ad.totalQty += tradeQty;
                ad.totalAmount += tradeQty * price;
                aggMap.put(activeApplSeqNum, ad);
            }
        }

        // 所有记录处理完毕后，对aggMap中数据进行打标签和汇总
        for (AggData ad : aggMap.values()) {
            double tradeVolumeRatio = (double) ad.totalQty / circulatingStock;
            String label = "";
            if (tradeVolumeRatio >= 0.003 || ad.totalQty >= 200000 || ad.totalAmount >= 1000000) {
                label = "超大";
            } else if (tradeVolumeRatio >= 0.001 || ad.totalQty >= 60000 || ad.totalAmount >= 300000) {
                label = "大";
            } else if (tradeVolumeRatio >= 0.00017 || ad.totalQty >= 10000 || ad.totalAmount >= 50000) {
                label = "中";
            } else {
                label = "小";
            }

            // 汇总
            if (ad.type == 1) { // 买单
                if (label.equals("超大")) {
                    ultraBuyQty += ad.totalQty;
                    ultraBuyAmount += ad.totalAmount;
                    mainInflow += ad.totalAmount;
                } else if (label.equals("大")) {
                    largeBuyQty += ad.totalQty;
                    largeBuyAmount += ad.totalAmount;
                    mainInflow += ad.totalAmount;
                } else if (label.equals("中")) {
                    midBuyQty += ad.totalQty;
                    midBuyAmount += ad.totalAmount;
                } else {
                    smallBuyQty += ad.totalQty;
                    smallBuyAmount += ad.totalAmount;
                }
            } else {
                // 卖单
                if (label.equals("超大")) {
                    ultraSellQty += ad.totalQty;
                    ultraSellAmount += ad.totalAmount;
                    mainOutflow += ad.totalAmount;
                } else if (label.equals("大")) {
                    largeSellQty += ad.totalQty;
                    largeSellAmount += ad.totalAmount;
                    mainOutflow += ad.totalAmount;
                } else if (label.equals("中")) {
                    midSellQty += ad.totalQty;
                    midSellAmount += ad.totalAmount;
                } else {
                    smallSellQty += ad.totalQty;
                    smallSellAmount += ad.totalAmount;
                }
            }
        }

        double netInflow = mainInflow - mainOutflow;
        String result = String.join("\t",
                String.valueOf(netInflow), String.valueOf(mainInflow), String.valueOf(mainOutflow),
                String.valueOf(ultraBuyQty), String.valueOf(ultraBuyAmount),
                String.valueOf(ultraSellQty), String.valueOf(ultraSellAmount),
                String.valueOf(largeBuyQty), String.valueOf(largeBuyAmount),
                String.valueOf(largeSellQty), String.valueOf(largeSellAmount),
                String.valueOf(midBuyQty), String.valueOf(midBuyAmount),
                String.valueOf(midSellQty), String.valueOf(midSellAmount),
                String.valueOf(smallBuyQty), String.valueOf(smallBuyAmount),
                String.valueOf(smallSellQty), String.valueOf(smallSellAmount)
        );

        context.write(key, new Text(result));
    }

    private void resetAggregation() {
        mainInflow = 0;
        mainOutflow = 0;
        ultraBuyQty = 0; ultraSellQty = 0;
        ultraBuyAmount = 0; ultraSellAmount = 0;
        largeBuyQty = 0; largeSellQty = 0;
        largeBuyAmount = 0; largeSellAmount = 0;
        midBuyQty = 0; midSellQty = 0;
        midBuyAmount = 0; midSellAmount = 0;
        smallBuyQty = 0; smallSellQty = 0;
        smallBuyAmount = 0; smallSellAmount = 0;
    }

    static class AggData {
        int type; // 1:买,0:卖
        long totalQty;
        double totalAmount;
        AggData(int type) {
            this.type = type;
        }
    }
}
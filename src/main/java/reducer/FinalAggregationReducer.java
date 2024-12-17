package reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class FinalAggregationReducer extends Reducer<Text, Text, Text, Text> {

    private HashMap<String,String> orderMap = new HashMap<>(); // applSeqNum -> transactTime
    private long circulatingStock; // 流通股本

    // 最终聚合结果
    private double mainInflow = 0; // 主力流入
    private double mainOutflow = 0; // 主力流出

    private long ultraBuyQty = 0, ultraSellQty = 0; // 超大单买入/卖出数量
    private double ultraBuyAmount = 0, ultraSellAmount = 0; // 超大单买入/卖出金额
    private long largeBuyQty = 0, largeSellQty = 0; // 大单买入/卖出数量
    private double largeBuyAmount = 0, largeSellAmount = 0; // 大单买入/卖出金额
    private long midBuyQty = 0, midSellQty = 0; // 中单买入/卖出数量
    private double midBuyAmount = 0, midSellAmount = 0; // 中单买入/卖出金额
    private long smallBuyQty = 0, smallSellQty = 0; // 小单买入/卖出数量
    private double smallBuyAmount = 0, smallSellAmount = 0; // 小单买入/卖出金额

    @Override
    protected void setup(Context context) throws IOException {
        this.circulatingStock = Long.parseLong(context.getConfiguration().get("circulatingStock")); // 设置流通股本为输入的股本数
        // 从DistributedCache中加载order数据
        /* Path[] cacheFiles = context.getLocalCacheFiles(); // 获取缓存文件
        if (cacheFiles != null) { // 如果缓存文件不为空
            for (Path cacheFile : cacheFiles) { // 遍历缓存文件
                try (BufferedReader reader = new BufferedReader(new FileReader(cacheFile.toString()))) { // 读取缓存文件
                    String line; // 定义字符串line
                    while ((line = reader.readLine()) != null) { // 逐行读取
                        int fieldCount = 2; // 预计字段数量
                        String[] fields = new String[fieldCount];
                        int start = 0, fieldIndex = 0;

                        for (int i = 0; i < line.length() && fieldIndex < fieldCount; i++) {
                            if (line.charAt(i) == '\t') {
                                fields[fieldIndex++] = line.substring(start, i);
                                start = i + 1;
                            }
                        }
                        if (fieldIndex < fieldCount) {
                            fields[fieldIndex++] = line.substring(start); // 最后一列
                        }// 按制表符分割字段
                        if (fields.length == 2) { // 检查字段数量是否满足要求
                            orderMap.put(fields[0], fields[1]); // applSeqNum -> transactTime 映射
                        }
                    }
                }
            }
        }*/
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

        for (Text val : values) { // 遍历values
            String[] fields = val.toString().split("\t"); // 按制表符分割字段
            if (fields.length ==16) { // 检查字段数量是否满足要求
                String bidApplSeqNum = fields[10]; // 获取bidApplSeqNum
                String offerApplSeqNum = fields[11]; // 获取offerApplSeqNum
                double price = Double.parseDouble(fields[12]); // 获取price
                long tradeQty = Long.parseLong(fields[13]); // 获取tradeQty

                /*String bidTime = orderMap.get(bidApplSeqNum); // 获取bidApplSeqNum对应的时间
                String offerTime = orderMap.get(offerApplSeqNum); // 获取offerApplSeqNum对应的时间
                if (bidTime == null || offerTime == null) {
                    // 找不到对应订单时间，跳过或继续处理
                    continue;
                }

                long bidTimeLong = Long.parseLong(bidTime);
                long offerTimeLong = Long.parseLong(offerTime);


                int type; // 1:买,0:卖
                if (bidTimeLong > offerTimeLong){
                    type = 1;
                } else if (bidTimeLong < offerTimeLong) {
                    type = 0;
                }else {
                    type = Long.parseLong(bidApplSeqNum) > Long.parseLong(offerApplSeqNum) ? 1 : 0;
                }*/
                int type = Long.parseLong(bidApplSeqNum) > Long.parseLong(offerApplSeqNum) ? 1 : 0; // 1买，0卖

                // 根据type确定使用的applSeqNum作key：如果type=1（买），主动方是bid方；如果type=0（卖），主动方是offer方
                String activeApplSeqNum = (type == 1) ? bidApplSeqNum : offerApplSeqNum;

                AggData ad = aggMap.getOrDefault(activeApplSeqNum, new AggData(type)); // 获取或初始化AggData
                ad.totalQty += tradeQty; // 累加tradeQty
                ad.totalAmount += tradeQty * price; // 累加tradeQty * price
                aggMap.put(activeApplSeqNum, ad); // 更新aggMap
            }
        }

        // 所有记录处理完毕后，对aggMap中数据进行打标签和汇总
        for (AggData ad : aggMap.values()) {
            double tradeVolumeRatio = (double) ad.totalQty / circulatingStock; // 成交量比例
            String label = "";
            if (tradeVolumeRatio >= 0.003 || ad.totalQty >= 200000 || ad.totalAmount >= 1000000) {
                label = "超大"; // 判断是否为超大单
            } else if (tradeVolumeRatio >= 0.001 || ad.totalQty >= 60000 || ad.totalAmount >= 300000) {
                label = "大"; // 判断是否为大单
            } else if (tradeVolumeRatio >= 0.00017 || ad.totalQty >= 10000 || ad.totalAmount >= 50000) {
                label = "中"; // 判断是否为中单
            } else {
                label = "小"; // 判断是否为小单
            }

            // 汇总
            if (ad.type == 1) { // 买单
                if (label.equals("超大")) {
                    ultraBuyQty += ad.totalQty; // 累加超大单买入数量
                    ultraBuyAmount += ad.totalAmount; // 累加超大单买入金额
                    mainInflow += ad.totalAmount; // 累加主力流入
                } else if (label.equals("大")) {
                    largeBuyQty += ad.totalQty; // 累加大单买入数量
                    largeBuyAmount += ad.totalAmount;  // 累加大单买入金额
                    mainInflow += ad.totalAmount; // 累加主力流入
                } else if (label.equals("中")) {
                    midBuyQty += ad.totalQty; // 累加中单买入数量
                    midBuyAmount += ad.totalAmount; // 累加中单买入金额
                } else {
                    smallBuyQty += ad.totalQty; // 累加小单买入数量
                    smallBuyAmount += ad.totalAmount; // 累加小单买入金额
                }
            } else {
                // 卖单
                if (label.equals("超大")) {
                    ultraSellQty += ad.totalQty; // 累加超大单卖出数量
                    ultraSellAmount += ad.totalAmount; // 累加超大单卖出金额
                    mainOutflow += ad.totalAmount; // 累加主力流出
                } else if (label.equals("大")) {
                    largeSellQty += ad.totalQty; // 累加大单卖出数量
                    largeSellAmount += ad.totalAmount; // 累加大单卖出金额
                    mainOutflow += ad.totalAmount; //累加主力流出
                } else if (label.equals("中")) {
                    midSellQty += ad.totalQty; // 累加中单卖出数量
                    midSellAmount += ad.totalAmount; // 累加中单卖出金额
                } else {
                    smallSellQty += ad.totalQty; // 累加小单卖出数量
                    smallSellAmount += ad.totalAmount; // 累加小单卖出金额
                }
            }
        }

        double netInflow = mainInflow - mainOutflow; // 计算净流入
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
        ); // 拼接结果字符串

        context.write(key, new Text(result));
    } // 输出timeWindow和最终聚合结果

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
    } // 重置聚合结果

    static class AggData {
        int type; // 1:买,0:卖
        long totalQty;
        double totalAmount;
        AggData(int type) {
            this.type = type;
        }
    } // 定义AggData类
}

package reducer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SumThenLabelReducer extends Reducer<Text, Text, Text, Text> {

    // 原job4中的聚合变量，用于主力净流入计算
    private double mainInflow = 0;
    private double mainOutflow = 0;
    private double netInflow = 0;

    private long ultraBuyQty = 0, ultraSellQty = 0;
    private double ultraBuyAmount = 0, ultraSellAmount = 0;
    private long largeBuyQty = 0, largeSellQty = 0;
    private double largeBuyAmount = 0, largeSellAmount = 0;
    private long midBuyQty = 0, midSellQty = 0;
    private double midBuyAmount = 0, midSellAmount = 0;
    private long smallBuyQty = 0, smallSellQty = 0;
    private double smallBuyAmount = 0, smallSellAmount = 0;

    private long circulatingStock;
    private String timeWindow;

    @Override
    protected void setup(Context context) {
        // 从配置中获取circulatingStock和时间窗口信息
        this.circulatingStock = Long.parseLong(context.getConfiguration().get("circulatingStock"));
        String timeStart = context.getConfiguration().get("timeStart");
        String timeEnd = context.getConfiguration().get("timeEnd");
        this.timeWindow = timeStart + "-" + timeEnd;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 先完成Job3的逻辑：对每个ApplSeqNum聚合、打标签
        // key为 ApplSeqNum+"+"+parts[4], values中为 (price, tradeQty, tradeType)
        // 这里与原SumThenLabelReducer逻辑相同

        long totalTradeQty = 0;
        double totalTradeAmount = 0;
        String tradeType = null;

        for (Text value : values) {
            String[] fields = value.toString().split("\t");
            double price = Double.parseDouble(fields[0]);
            long qty = Long.parseLong(fields[1]);
            tradeType = fields[2];

            totalTradeQty += qty;
            totalTradeAmount += qty * price;
        }

        double tradeVolumeRatio = (double) totalTradeQty / circulatingStock;
        String label = "";
        if (tradeVolumeRatio >= 0.003 || totalTradeQty >= 200000 || totalTradeAmount >= 1000000) {
            label = "超大";
        } else if (tradeVolumeRatio >= 0.001 || totalTradeQty >= 60000 || totalTradeAmount >= 300000) {
            label = "大";
        } else if (tradeVolumeRatio >= 0.00017 || totalTradeQty >= 10000 || totalTradeAmount >= 50000) {
            label = "中";
        } else {
            label = "小";
        }

        // 到这里是Job3的原始输出逻辑(之前会直接context.write(key, ...))
        // 现在不直接输出，而是将结果计入Job4的逻辑中

        // 原Job4逻辑: 根据label和tradeType将交易额、量计入相应区域
        // tradeType中存放的是 "0" 或 "1"
        int tType = Integer.parseInt(tradeType);
        if (tType == 1) { // 买单
            if (label.equals("超大")) {
                ultraBuyQty += totalTradeQty;
                ultraBuyAmount += totalTradeAmount;
                mainInflow += totalTradeAmount;
            } else if (label.equals("大")) {
                largeBuyQty += totalTradeQty;
                largeBuyAmount += totalTradeAmount;
                mainInflow += totalTradeAmount;
            } else if (label.equals("中")) {
                midBuyQty += totalTradeQty;
                midBuyAmount += totalTradeAmount;
            } else if (label.equals("小")) {
                smallBuyQty += totalTradeQty;
                smallBuyAmount += totalTradeAmount;
            }
        } else { // 卖单
            if (label.equals("超大")) {
                ultraSellQty += totalTradeQty;
                ultraSellAmount += totalTradeAmount;
                mainOutflow += totalTradeAmount;
            } else if (label.equals("大")) {
                largeSellQty += totalTradeQty;
                largeSellAmount += totalTradeAmount;
                mainOutflow += totalTradeAmount;
            } else if (label.equals("中")) {
                midSellQty += totalTradeQty;
                midSellAmount += totalTradeAmount;
            } else if (label.equals("小")) {
                smallSellQty += totalTradeQty;
                smallSellAmount += totalTradeAmount;
            }
        }

        // 不在此处输出，因为我们还要等所有记录处理完才能计算最终净流入和输出最终结果
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 所有记录处理完毕后，计算主力净流入并输出最终结果（相当于原Job4的输出）
        netInflow = mainInflow - mainOutflow;
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
        // 输出最终结果，key为timeWindow
        context.write(new Text(timeWindow), new Text(result));
    }
}
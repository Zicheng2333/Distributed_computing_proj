package reducer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FinalCalculationReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double mainInflow = 0;   // 主力流入金额
        double mainOutflow = 0; // 主力流出金额
        double netInflow = 0;   // 主力净流入金额

        long ultraBuyQty = 0, ultraSellQty = 0; // 超大买单和卖单成交量
        double ultraBuyAmount = 0, ultraSellAmount = 0; // 超大买单和卖单成交额
        long largeBuyQty = 0, largeSellQty = 0; // 大买单和卖单成交量
        double largeBuyAmount = 0, largeSellAmount = 0; // 大买单和卖单成交额
        long midBuyQty = 0, midSellQty = 0; // 中买单和卖单成交量
        double midBuyAmount = 0, midSellAmount = 0; // 中买单和卖单成交额
        long smallBuyQty = 0, smallSellQty = 0; // 小买单和卖单成交量
        double smallBuyAmount = 0, smallSellAmount = 0; // 小买单和卖单成交额

        for (Text value : values) {
            String[] fields = value.toString().split("\t");
            if (fields.length != 4) {
                continue; // 跳过格式不正确的数据
            }


            long tradeQty = Long.parseLong(fields[0]); // 成交量
            double tradeAmount = Double.parseDouble(fields[1]); // 成交额
            String label = fields[2]; // 分类标签
            String tradeType = fields[3]; // 买/卖

            // 汇总主力流入与流出
            if (Integer.parseInt(tradeType) == 1) {
                mainInflow += tradeAmount;
                if (label.equals("超大")) {
                    ultraBuyQty += tradeQty;
                    ultraBuyAmount += tradeAmount;
                } else if (label.equals("大")) {
                    largeBuyQty += tradeQty;
                    largeBuyAmount += tradeAmount;
                } else if (label.equals("中")) {
                    midBuyQty += tradeQty;
                    midBuyAmount += tradeAmount;
                } else if (label.equals("小")) {
                    smallBuyQty += tradeQty;
                    smallBuyAmount += tradeAmount;
                }
            } else if (Integer.parseInt(tradeType) == 0) {
                mainOutflow += tradeAmount;
                if (label.equals("超大")) {
                    ultraSellQty += tradeQty;
                    ultraSellAmount += tradeAmount;
                } else if (label.equals("大")) {
                    largeSellQty += tradeQty;
                    largeSellAmount += tradeAmount;
                } else if (label.equals("中")) {
                    midSellQty += tradeQty;
                    midSellAmount += tradeAmount;
                } else if (label.equals("小")) {
                    smallSellQty += tradeQty;
                    smallSellAmount += tradeAmount;
                }
            }
        }

        // 计算主力净流入
        netInflow = mainInflow - mainOutflow;

        // 输出
        String result = String.join("\t",
                String.valueOf(mainInflow), String.valueOf(mainOutflow), String.valueOf(netInflow),
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
}

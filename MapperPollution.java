/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pm10;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;

/**
 *
 * @author Alessandro
 */
class MapperPollution extends Mapper<Text, // Input key type
        Text, // Input value type
        NullWritable, // Output key type
        DateIncome> {// Output value type

    private List<DateIncome> topKDateIncome;
    private int k;

    @Override
    protected void setup(Context context) {
        // for each mapper, top1 is used to store the information about the topDate
        // date of the subset of lines analyzed by the mapper
        topKDateIncome = new LinkedList<>();
        k = Integer.parseInt(context.getConfiguration().get("knumber"));
    }

    @Override
    protected void map(Text key, // Input key type
            Text value, // Input value type
            Context context) throws IOException, InterruptedException {

        String date = key.toString();

        float dailyIncome = Float.parseFloat(value.toString());

        DateIncome di = new DateIncome(date, dailyIncome);

        if (!topKDateIncome.contains(di)) {
            topKDateIncome.add(di);
        }
        topKDateIncome.sort(
                (o1, o2) -> o2.getIncome().compareTo(o1.getIncome())
        );

        if (topKDateIncome.size() > k) {
            topKDateIncome.subList(k, topKDateIncome.size()).clear();
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Emit the top1 date and income related to this mapper
        for (DateIncome d : topKDateIncome) {
            context.write(NullWritable.get(), d);
        }

    }

}

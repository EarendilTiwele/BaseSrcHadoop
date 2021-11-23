/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pm10;

import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.io.*;

/**
 *
 * @author Alessandro
 */
class ReducerPollution extends Reducer<NullWritable, // Input key type
        DateIncome, // Input value type
        Text, // Output key type
        FloatWritable> { // Output value type

    private int k;

    @Override
    protected void setup(Context context) {

        k = Integer.parseInt(context.getConfiguration().get("knumber"));
    }

    // The reduce method is called only once in this approach
    // All the key-value pairs emitted by the mappers have the
    // same key (NullWritable.get())
    @Override
    protected void reduce(NullWritable key, // Input key type
            Iterable<DateIncome> values, // Input value type
            Context context) throws IOException, InterruptedException {

        List<DateIncome> globalTopDateIncome = new LinkedList<>();

        // Iterate over the set of values and select the top 3 income and
        // the related date
        for (DateIncome value : values) {

            DateIncome di = new DateIncome(value.getDate(), value.getIncome());

            int index = globalTopDateIncome.indexOf(di);

            if (index == -1) {
                globalTopDateIncome.add(di);
            } else {

                DateIncome dc = globalTopDateIncome.get(index);
                if (dc.getDate().compareToIgnoreCase(di.getDate()) > 0) {
                    dc.setDate(di.getDate());
                }

            }

        }

        globalTopDateIncome.sort((o1, o2) -> o2.getIncome().compareTo(o1.getIncome()));

        if (globalTopDateIncome.size() > k) {
            globalTopDateIncome.subList(k, globalTopDateIncome.size()).clear();
        }

        // Emit pair (date, income) associated with top 1 income
        for (DateIncome d : globalTopDateIncome) {
            context.write(new Text(d.getDate()), new FloatWritable(d.getIncome()));
        }
    }

}

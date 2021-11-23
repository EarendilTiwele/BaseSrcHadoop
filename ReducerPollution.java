/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pm10;

import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.*;

/**
 *
 * @author Alessandro
 */
class ReducerPollution extends Reducer<Text, // Input key type
        IntWritable, // Input value type
        Text, // Output key type
        IntWritable> { // Output value type

    @Override
    protected void reduce(Text key, // Input key type
            Iterable<IntWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {

        int numDays = 0;

        // Iterate over the set of values and sum them
        for (IntWritable value : values) {
            numDays += value.get();
        }

        context.write(new Text(key), new IntWritable(numDays));
    }

}

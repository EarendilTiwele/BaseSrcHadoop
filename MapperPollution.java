/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pm10;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;

/**
 *
 * @author Alessandro
 */
class MapperPollution extends Mapper<Text, // Input key type
        Text, // Input value type
        Text, // Output key type
        IntWritable> {// Output value type

    private final static Double PM10T = new Double(50);
    private final static IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(Text key, // Input key type
            Text value, // Input value type
            Context context) throws IOException, InterruptedException {

        // Extract sensor and date from key
        String[] fields = key.toString().split(",");

        String sensor_id = fields[0];
        Double PM10Level = new Double(value.toString());

        // Compare the value of PM10 with the threshold value
        if (PM10Level.compareTo(PM10T) > 0) {
            // Emit the pair (sensor_id, 1)
            context.write(new Text(sensor_id), ONE);
        }

    }

}

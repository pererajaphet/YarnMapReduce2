package com.opstty.reducer;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.stream.StreamSupport;



public class MaxHeightKindsReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
            context.write(key, new FloatWritable(StreamSupport.stream(values.spliterator(), false)
                    .map((v) -> { return v.get(); }).
                    max(Float::compare).get()));
        }
    }


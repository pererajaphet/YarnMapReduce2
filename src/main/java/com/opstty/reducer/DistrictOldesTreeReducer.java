package com.opstty.reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


import org.apache.hadoop.mapreduce.Reducer;

public class DistrictOldesTreeReducer extends Reducer<NullWritable, MapWritable, IntWritable, IntWritable> {
    public void reduce(NullWritable key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {
        ArrayList<Integer[]> district_trees = (ArrayList<Integer[]>) StreamSupport.stream(values.spliterator(), false)
                .map( mw ->  (new Integer[] { ((IntWritable) mw.keySet().toArray()[0]).get(), ((IntWritable) mw.get(mw.keySet().toArray()[0])).get() }))
                .collect(Collectors.toList());
        int max_trees = district_trees.stream().map((arr) -> arr[1]).max(Integer::compare).get();

        district_trees.stream().filter(arr -> arr[1] == max_trees).map(arr -> arr[0]).distinct().forEach((district) -> { try {
            context.write(new IntWritable(district), new IntWritable(max_trees));
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } });
    }
}
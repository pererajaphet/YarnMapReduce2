package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SpeciesMapperTest {
    @Mock
    private Mapper.Context context;
    private SpeciesMapper SpeciesMapper;

    @Before
    public void setup() {
        this.SpeciesMapper = new SpeciesMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "(48.8373323894, 2.40776275516);12;Celis;Nirol;Meryil;1803;19.0;;hqki;hfyd;6;jkj";
        this.SpeciesMapper.map(null, new Text(value), this.context);
        verify(this.context, times(3))
                .write(new Text("Nirgicia"), new IntWritable(1));
    }
}
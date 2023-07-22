package org.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class VenteMapper extends Mapper<LongWritable, Text,Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
     //ca correspond au chaque ligne du fichier ventes.txt
        String ventes[] =value.toString().split(" ");
        String ville=ventes[1];
        String dates=ventes[0];
        String date[]=dates.split("-");
        String year=date[2];
        double prix= Double.parseDouble(ventes[3]);
            context.write(new Text(year+","+ville),new DoubleWritable(prix));


    }
}
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VentesMapper extends Mapper<LongWritable, Text,Text,BooleanWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(" ");
        String AddressIP = parts[0];
        String StatusRequest=parts[8];

        Boolean StatusSucces=false;
        if(StatusRequest.equals("200")){
            StatusSucces=true;
        }
        //        String ville = parts[1];


        //        float prix = Float.parseFloat(parts[3]);

        context.write(new Text(AddressIP), new BooleanWritable(StatusSucces));
    }

}

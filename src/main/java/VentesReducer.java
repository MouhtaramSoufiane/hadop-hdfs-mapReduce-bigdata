import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class VentesReducer extends Reducer<Text, BooleanWritable,Text,Text> {
    public void reduce(Text key, Iterable<BooleanWritable> values, Context context)
        throws IOException, InterruptedException {
    Integer totalRequest = 0;
    Integer totalHttpSucess = 0;
    for (BooleanWritable value : values) {
        String s = value.toString();
        System.out.println(s);
          if(s=="true"){
              totalHttpSucess+=1;
          }
            totalRequest += 1;



    }
    context.write(key , new Text(totalRequest+"      "+totalHttpSucess));
}
}

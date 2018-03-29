package mx.iteso.desi.cloud.hw2;

import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.io.*;

import mx.iteso.desi.cloud.GeocodeWritable;

public class GeocodeReducer extends Reducer<Text, GeocodeWritable, Text, Text> {

  public void reduce(Text key, Iterable<GeocodeWritable> values, Context context) 
	throws java.io.IOException, InterruptedException{
		
		boolean isRelatedToCities = false;
		double[] coordinates = new double[2];
		
		for (GeocodeWritable value : values) {
			if(value.getName().toString().equals("GEOCODE")) {
				isRelatedToCities = true;
				coordinates[0] = value.getLatitude();
				coordinates[1] = value.getLongitude();
				break;
      }
    }
    
		if(!isRelatedToCities) return;
		String coordsToString = "(" + coordinates[0] + "," + coordinates[1] + ")";
    
		for (GeocodeWritable value : values) {
			if(!value.getName().toString().equals("GEOCODE")) {
        String keyAndURL = key.toString() + "\t" + value.getName().toString();

				context.write(new Text(coordsToString), new Text(keyAndURL));
			}
    }
	}
  
}

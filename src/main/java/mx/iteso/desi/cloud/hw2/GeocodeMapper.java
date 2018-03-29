package mx.iteso.desi.cloud.hw2;

import mx.iteso.desi.cloud.GeocodeWritable;
import mx.iteso.desi.cloud.Geocode;
import mx.iteso.desi.cloud.Triple;
import mx.iteso.desi.cloud.ParseTriple;
import mx.iteso.desi.cloud.ParserCoordinates;

import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

import org.apache.hadoop.io.*;

public class GeocodeMapper extends Mapper<LongWritable, Text, Text, GeocodeWritable> {

    private final static double[] PHL_COORDS = {39.88, -75.25};
    private final static double[] HOU_COORDS = {29.97, -95.35};
    private final static double[] SEA_COORDS = {47.45, -122.30};
    private final static double[] GDL_COORDS = {20.66, -103.39};
    private final static double[] MTY_COORDS = {25.67, -100.31};
    private final static double[][] ALL_COORDS = {PHL_COORDS, HOU_COORDS, SEA_COORDS, GDL_COORDS, MTY_COORDS};

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value == null) return;
        Triple triple = ParseTriple.parseTriple(value.toString());
        if (triple == null) return;
        GeocodeWritable geocodeWritable = new GeocodeWritable();

        if (triple.getRelationship().equals("http://xmlns.com/foaf/0.1/depiction")) {
            geocodeWritable.set(new Text(triple.getObject()), new DoubleWritable(0), new DoubleWritable(0));
        } else if (triple.getRelationship().equals("http://www.georss.org/georss/point")) {
            Double [] coordinates = ParserCoordinates.parseCoordinates(triple.getObject());
            if (coordinates == null) return;

            Geocode g = new Geocode(null, coordinates[0], coordinates[1]);
            if(!isCloseToCity(g)) return;
            
            geocodeWritable.set(new Text("GEOCODE"),new DoubleWritable(coordinates[0]), new DoubleWritable(coordinates[1]));
        } else {
            return;
        }
        context.write(new Text(triple.getSubject()), geocodeWritable);
    }

    private static boolean isCloseToCity(Geocode g) {
        for (int i = 0; i < ALL_COORDS.length; i++) {
            double latitude = ALL_COORDS[i][0];
            double longitude = ALL_COORDS[i][1];
            double distInMeters = g.getHaversineDistance(latitude, longitude);
            if(distInMeters <= 5000)
                return true;
        }
        return false;
    }
  
}

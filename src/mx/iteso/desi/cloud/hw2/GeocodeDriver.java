package mx.iteso.desi.cloud.hw2;

import mx.iteso.desi.cloud.Triple;

public class GeocodeDriver {

  public static void main(String[] args) throws Exception {
  
    if (args.length != 2) {
      System.err.println("Usage: GeocodeDriver <input path> <output path>");
      System.exit(-1);
    }
    
    /* TODO: Your driver code here */
	
  }
  
  public static Triple parseTriple(String str) {
    try {
      int subjLAngle = 0;
      int subjRAngle = str.indexOf('>');
      int predLAngle = str.indexOf('<', subjRAngle + 1);
      int predRAngle = str.indexOf('>', predLAngle + 1);
      int objLAngle = str.indexOf('<', predRAngle + 1);
      int objRAngle = str.indexOf('>', objLAngle + 1);

      if (objLAngle == -1) {
	objLAngle = str.indexOf('\"', predRAngle + 1);
	objRAngle = str.indexOf('\"', objLAngle + 1); 
      }

      String subject = str.substring(subjLAngle + 1, subjRAngle);
      String predicate = str.substring(predLAngle + 1, predRAngle);
      String object = str.substring(objLAngle + 1, objRAngle);

      return new Triple(subject, predicate, object);
    } catch(Exception e) {
      return null;
    }
  }
	
  public static Double[] parseCoordinates(String raw) {
    int space = raw.indexOf(' ');
    String latStr = raw.substring(0, space);
    String lonStr = raw.substring(space + 1);
    Double lat = Double.parseDouble(latStr);
    Double lon = Double.parseDouble(lonStr);
    if (lat == null || lon == null)
      return null;
 
    return new Double[] {lat, lon};
  }
}

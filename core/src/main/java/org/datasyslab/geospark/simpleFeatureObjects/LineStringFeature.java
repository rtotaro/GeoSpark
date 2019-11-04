package org.datasyslab.geospark.simpleFeatureObjects;

import org.geotools.data.DataUtilities;
import org.locationtech.jts.geom.LineString;
import org.opengis.feature.simple.SimpleFeature;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class LineStringFeature extends GeometryFeature<LineString> {

    private LineStringFeature() {
        super(null);
    }

    private LineStringFeature(SimpleFeature delegate) {
        super(delegate);
    }

    public static LineStringFeature createFeature(SimpleFeature feature, LineString point)
    {
        feature.setDefaultGeometry(point);
        return new LineStringFeature(feature);
    }

    public static LineStringFeature createFeature(SimpleFeature feature)
    {
        Object defaultGeometry = feature.getDefaultGeometry();
        if(defaultGeometry instanceof LineString)
        {
            feature.setDefaultGeometry(defaultGeometry);
            return new LineStringFeature(feature);
        }

        throw new IllegalArgumentException("default geometry in feature is not a LineString");

    }
    private void readObject(ObjectInputStream aInputStream) throws ClassNotFoundException, IOException {
        int available = aInputStream.available();
        byte[] bytes = new byte[available];
        aInputStream.read(bytes,0,available);
        SimpleFeature feature = DataUtilities.createFeature(geometryFeatureType, new String(bytes));
        super.delegate = feature;

    }

    private void writeObject(ObjectOutputStream aOutputStream) throws IOException {

        String encodeFeature = DataUtilities.encodeFeature(this,true);
        aOutputStream.write(encodeFeature.getBytes());
    }
}

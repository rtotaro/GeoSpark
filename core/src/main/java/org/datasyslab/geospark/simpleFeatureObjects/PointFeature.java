package org.datasyslab.geospark.simpleFeatureObjects;

import org.geotools.data.DataUtilities;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class PointFeature extends GeometryFeature<Point> implements Serializable {

    private PointFeature() {
        super(null);
    }

    private PointFeature(SimpleFeature delegate) {
        super(delegate);
    }

    public static PointFeature createFeature(SimpleFeature feature, Point point)
    {
        feature.setDefaultGeometry(point);
        return new PointFeature(feature);
    }

    public static PointFeature createFeature(SimpleFeature feature) {
        Object defaultGeometry = feature.getDefaultGeometry();
        if (defaultGeometry instanceof Point) {
            feature.setDefaultGeometry(defaultGeometry);
            return new PointFeature(feature);
        }

        throw new IllegalArgumentException("default geometry in feature is not a point");

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

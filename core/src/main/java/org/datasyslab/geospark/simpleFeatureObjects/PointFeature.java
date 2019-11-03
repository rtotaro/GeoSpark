package org.datasyslab.geospark.simpleFeatureObjects;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class PointFeature extends GeometryFeature<Point> {
    private PointFeature(SimpleFeature delegate) {
        super(delegate);
    }

    public static PointFeature createFeature(SimpleFeature feature, Point point)
    {
        feature.setDefaultGeometry(point);
        return new PointFeature(feature);
    }

    public static PointFeature createFeature(SimpleFeature feature)
    {
        Object defaultGeometry = feature.getDefaultGeometry();
        if(defaultGeometry instanceof Point)
        {
            feature.setDefaultGeometry(defaultGeometry);
            return new PointFeature(feature);
        }

        throw new IllegalArgumentException("default geometry in feature is not a point");

    }

//    //Setters and Getters
//
//    private void readObject(ObjectInputStream aInputStream) throws ClassNotFoundException, IOException {
//
//    }
//
//    private void writeObject(ObjectOutputStream aOutputStream) throws IOException {
//
//    }

}

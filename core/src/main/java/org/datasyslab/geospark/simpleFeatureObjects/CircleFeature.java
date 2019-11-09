package org.datasyslab.geospark.simpleFeatureObjects;

import org.datasyslab.geospark.geometryObjects.Circle;
import org.geotools.data.DataUtilities;
import org.opengis.feature.simple.SimpleFeature;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class CircleFeature extends GeometryFeature<Circle> implements Serializable {

    private CircleFeature() {
        super(null);
    }

    private CircleFeature(SimpleFeature delegate) {
        super(delegate);
    }

    public static CircleFeature createFeature(SimpleFeature feature, Circle point)
    {
        feature.setDefaultGeometry(point);
        return new CircleFeature(feature);
    }

    public static CircleFeature createFeature(SimpleFeature feature)
    {
        Object defaultGeometry = feature.getDefaultGeometry();
        if(defaultGeometry instanceof Circle)
        {
            feature.setDefaultGeometry(defaultGeometry);
            return new CircleFeature(feature);
        }

        throw new IllegalArgumentException("default geometry in feature is not a circle");

    }


}

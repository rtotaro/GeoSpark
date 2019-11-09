package org.datasyslab.geospark.simpleFeatureObjects;

import org.geotools.data.DataUtilities;
import org.locationtech.jts.geom.Polygon;
import org.opengis.feature.simple.SimpleFeature;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class PolygonFeature extends GeometryFeature<Polygon> implements Serializable {

    private PolygonFeature() {
        super(null);
    }

    private PolygonFeature(SimpleFeature delegate) {
        super(delegate);
    }

    public static PolygonFeature createFeature(SimpleFeature feature, Polygon polygon)
    {
        feature.setDefaultGeometry(polygon);
        return new PolygonFeature(feature);
    }

    public static PolygonFeature createFeature(SimpleFeature feature)
    {
        Object defaultGeometry = feature.getDefaultGeometry();
        if(defaultGeometry instanceof Polygon)
        {
            return new PolygonFeature(feature);
        }

        throw new IllegalArgumentException("default geometry in feature is not a Polygon");

    }

}

package org.datasyslab.geospark.simpleFeatureObjects;

import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.opengis.feature.simple.SimpleFeature;

public class RectangleFeature extends GeometryFeature<Polygon> {
    private RectangleFeature(SimpleFeature delegate) {
        super(delegate);
    }

    public static RectangleFeature createFeature(SimpleFeature feature, Polygon point)
    {
        feature.setDefaultGeometry(point);
        return new RectangleFeature(feature);
    }

    public static RectangleFeature createFeature(SimpleFeature feature)
    {
        Object defaultGeometry = feature.getDefaultGeometry();
        if(defaultGeometry instanceof Polygon)
        {
            feature.setDefaultGeometry(defaultGeometry);
            return new RectangleFeature(feature);
        }

        throw new IllegalArgumentException("default geometry in feature is not a Polygon");

    }
}

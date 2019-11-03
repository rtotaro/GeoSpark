package org.datasyslab.geospark.simpleFeatureObjects;

import org.datasyslab.geospark.geometryObjects.Circle;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;

public class LineStringFeature extends GeometryFeature<LineString> {
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
}

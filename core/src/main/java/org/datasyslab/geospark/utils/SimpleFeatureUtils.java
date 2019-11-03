package org.datasyslab.geospark.utils;

import org.datasyslab.geospark.geometryObjects.Circle;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTS;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import java.util.function.Function;

public class SimpleFeatureUtils {


    public static <T extends SimpleFeature> T transform (T simpleFeature,final MathTransform transform) throws TransformException {
        Geometry defaultGeometry = (Geometry) simpleFeature.getDefaultGeometry();
        T copy = (T) SimpleFeatureBuilder.copy(simpleFeature);
        copy.setDefaultGeometry(JTS.transform(defaultGeometry,transform));
        return copy;
    }

    public static <T extends SimpleFeature,G1 extends Geometry,G2 extends Geometry> T transform (T simpleFeature,final Function<G1,G2 > transform) {
        G1 defaultGeometry = (G1) simpleFeature.getDefaultGeometry();
        T copy = (T) SimpleFeatureBuilder.copy(simpleFeature);
        copy.setDefaultGeometry(transform.apply(defaultGeometry));
        return copy;
    }


    public static Envelope getEnvelopeInternal(SimpleFeature simpleFeature){
        return ((Geometry)simpleFeature).getEnvelopeInternal();
    }


}

package org.datasyslab.geospark.simpleFeatureObjects;

import org.geotools.feature.DecoratingFeature;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTS;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

public abstract class GeometryFeature<T extends Geometry> extends DecoratingFeature {
    protected GeometryFeature(SimpleFeature delegate) {
        super(delegate);
        getUserData().put("geomData",getDefaultGeometry().getUserData());
    }

    public T getDefaultGeometry() {
        T defaultGeometry = (T) delegate.getDefaultGeometry();
        defaultGeometry.setUserData(getUserData().get("geomData"));
        return defaultGeometry;
    }

    public Envelope getEnvelopeInternal(){
        return getDefaultGeometry().getEnvelopeInternal();
    }


    public GeometryFeature transform (final MathTransform transform) throws TransformException {
        Geometry defaultGeometry = getDefaultGeometry();
        GeometryFeature copy = (GeometryFeature) SimpleFeatureBuilder.copy(this);
        copy.setDefaultGeometry(JTS.transform(defaultGeometry,transform));
        return copy;
    }
}

package org.datasyslab.geospark.simpleFeatureObjects;

import org.datasyslab.geospark.geometryObjects.Circle;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTS;
import org.locationtech.jts.geom.*;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import java.io.Serializable;
import java.util.function.Function;

public abstract class GeometryFeature<T extends Geometry> extends DecoratingFeature implements Serializable {

    protected GeometryFeature(SimpleFeature delegate) {
        super(delegate);
        if (delegate == null) {
            throw new IllegalArgumentException("DelegateFeature cannot be null");
        }
        Object userData = ((Geometry) delegate.getDefaultGeometry()).getUserData();
        setGeomData(userData);
    }

    public T getDefaultGeometry() {
        T defaultGeometry = (T) delegate.getDefaultGeometry();
        defaultGeometry.setUserData(getGeomData());
        return defaultGeometry;
    }

    public Envelope getEnvelopeInternal() {
        return getDefaultGeometry().getEnvelopeInternal();
    }


    public GeometryFeature transform(final MathTransform transform) throws TransformException {
        Geometry defaultGeometry = getDefaultGeometry();
        GeometryFeature copy = (GeometryFeature) createGeometryFeature(SimpleFeatureBuilder.copy(this));
        copy.setDefaultGeometry(JTS.transform(defaultGeometry, transform));
        copy.setGeomData(getGeomData());
        return copy;
    }

    public GeometryFeature transform(final Function<Geometry, Geometry> transform) {
        Geometry defaultGeometry = getDefaultGeometry();
        GeometryFeature copy = (GeometryFeature) createGeometryFeature(SimpleFeatureBuilder.copy(this));
        copy.setDefaultGeometry(transform.apply((T) defaultGeometry));
        copy.setGeomData(getGeomData());
        return copy;
    }

    public boolean intersects(GeometryFeature geometryFeature) {
        return this.getDefaultGeometry().intersects(geometryFeature.getDefaultGeometry());
    }

    void setGeomData(Object geomData) {
        getUserData().put("geomData", geomData);
    }

    public Object getGeomData() {
        return getUserData().get("geomData");
    }

    public static GeometryFeature createGeometryFeature(SimpleFeature sf) {
        Object geom = sf.getDefaultGeometry();
        if (geom instanceof Point) {
            Point point = (Point) geom;
            return PointFeature.createFeature(sf);
        } else if (geom instanceof Polygon) {
            Polygon polygon = (Polygon) geom;
            return PolygonFeature.createFeature(sf);

        } else if (geom instanceof LineString) {
            LineString lineS = (LineString) geom;
            return LineStringFeature.createFeature(sf);

        } else if (geom instanceof Circle) {
            Circle circle = (Circle) geom;
            return CircleFeature.createFeature(sf);
        }
        else if (geom instanceof GeometryCollection) {
            GeometryCollection geometryCollection = (GeometryCollection) geom;
            return GeometryCollectionFeature.createFeature(sf);

        }else {
            throw new IllegalArgumentException("Unsupported geometry:" + geom.getClass().toString());

        }
    }



}

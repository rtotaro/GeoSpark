package org.datasyslab.geospark.simpleFeatureObjects;

import org.datasyslab.geospark.geometryObjects.Circle;
import org.geotools.data.DataUtilities;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTS;
import org.locationtech.geomesa.features.kryo.serialization.SimpleFeatureSerializer;
import org.locationtech.jts.geom.*;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import scala.collection.immutable.HashSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.UUID;
import java.util.function.Function;

public abstract class GeometryFeature<T extends Geometry> extends DecoratingFeature implements Serializable {

    protected GeometryFeature(SimpleFeature delegate) {
        super(delegate);
        setGeomData(getDefaultGeometry().getUserData());
    }

    public static SimpleFeatureType geometryFeatureType;

    public static SimpleFeatureSerializer serializer;

    static {
        SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
        simpleFeatureTypeBuilder.setName("GeometryFeature");
        simpleFeatureTypeBuilder.setDefaultGeometry("geom");

        AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder().binding(String.class);
        attributeTypeBuilder.setName("wkt");

        AttributeTypeBuilder geomAttributeBuilder = new AttributeTypeBuilder().binding(Geometry.class);
        attributeTypeBuilder.setName("geom");

        simpleFeatureTypeBuilder.addAll(new AttributeDescriptor[]{attributeTypeBuilder.buildDescriptor("wkt"), geomAttributeBuilder.buildDescriptor("geom")});
        geometryFeatureType = simpleFeatureTypeBuilder.buildFeatureType();

        serializer = new SimpleFeatureSerializer(geometryFeatureType, new HashSet<>());
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

    private void setGeomData(Object geomData) {
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

        } else {
            throw new IllegalArgumentException("Unsupported geometry:" + geom.getClass().toString());

        }
    }

    public static GeometryFeature createGeometryFeature(Geometry geom) {
        return createGeometryFeature(geom, UUID.randomUUID().toString());
    }

    public static GeometryFeature createGeometryFeature(Geometry geom, String featureId) {
        SimpleFeature simpleFeature = createSimpleFeature(geom, featureId);
        GeometryFeature result = null;
        if (geom instanceof Point) {
            Point point = (Point) geom;
            result = PointFeature.createFeature(simpleFeature);
        } else if (geom instanceof Polygon) {
            Polygon polygon = (Polygon) geom;
            result = PolygonFeature.createFeature(simpleFeature);

        } else if (geom instanceof LineString) {
            LineString lineS = (LineString) geom;
            result = LineStringFeature.createFeature(simpleFeature);

        } else if (geom instanceof Circle) {
            Circle circle = (Circle) geom;
            result = CircleFeature.createFeature(simpleFeature);

        } else {
            throw new IllegalArgumentException("Unsupported geometry:" + geom.getClass().toString());

        }

        result.setGeomData(geom.getUserData());
        return result;
    }


    private static SimpleFeature createSimpleFeature(Geometry geometry, String id) {

        String wkt = null;
        if (geometry instanceof Circle) {
            Circle circle = (Circle) geometry;
            Point point = circle.getCentroid();
            wkt = point.buffer(circle.getRadius()).toText();
        } else {
            wkt = geometry.toText();
        }

        SimpleFeature simpleFeature = SimpleFeatureBuilder.build(geometryFeatureType, Arrays.asList(wkt, geometry), UUID.randomUUID().toString());
        return simpleFeature;
    }

    private static SimpleFeature createSimpleFeature(Geometry geometry) {

        return createSimpleFeature(geometry, UUID.randomUUID().toString());
    }


    private void readObject(ObjectInputStream aInputStream) throws ClassNotFoundException, IOException {
        int available = aInputStream.available();
        byte[] bytes = new byte[available];
        aInputStream.read(bytes,0,available);
        SimpleFeature feature = DataUtilities.createFeature(geometryFeatureType, new String(bytes));
        super.delegate = feature;

    }

    private void writeObject(ObjectOutputStream aOutputStream) throws IOException {

        String encodeFeature = DataUtilities.encodeFeature(this.delegate,true);
        aOutputStream.write(encodeFeature.getBytes());
    }





}

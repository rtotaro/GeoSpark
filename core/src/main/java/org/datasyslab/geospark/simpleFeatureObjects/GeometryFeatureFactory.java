package org.datasyslab.geospark.simpleFeatureObjects;

import org.datasyslab.geospark.geometryObjects.Circle;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.jts.geom.*;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class GeometryFeatureFactory {

    public static GeometryFeature createGeometryFeature(Geometry geom) {
        return createGeometryFeature(geom, UUID.randomUUID().toString());
    }

    public static GeometryFeature createGeometryFeature(Geometry geom,SimpleFeatureType sft) {
        return createGeometryFeature(geom, UUID.randomUUID().toString());
    }

    public static SimpleFeatureType geometryFeatureType;

//    public static SimpleFeatureSerializer serializer;

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

//        serializer = new SimpleFeatureSerializer(geometryFeatureType, new HashSet<>());
    }

    public static List<? extends GeometryFeature> createGeometryFeatures(Geometry geom, Class<? extends Geometry> targetGeometries) {

        List<GeometryFeature> result = new ArrayList<>();

        if (geom instanceof GeometryCollection) {
            GeometryCollection multiObjects = (GeometryCollection) geom;
            for (int i = 0; i < multiObjects.getNumGeometries(); i++) {
                Geometry oneObject = (Geometry) multiObjects.getGeometryN(i);
                if (!targetGeometries.isAssignableFrom(oneObject.getClass())) {
                    throw new IllegalArgumentException(MessageFormat.format("Cannot convert {0} to {1}", oneObject.getClass(), targetGeometries));
                }
                oneObject.setUserData(multiObjects.getUserData());
                result.add(createGeometryFeature(oneObject));
            }
        } else {
            result.add(createGeometryFeature(geom));
        }
        return result;
    }

    public static GeometryFeature createGeometryFeature(Geometry geom, String featureId) {
        return createGeometryFeature(geom, featureId,geometryFeatureType);
    }
    public static GeometryFeature createGeometryFeature(Geometry geom, String featureId,SimpleFeatureType sft) {
        SimpleFeature simpleFeature = createSimpleFeature(geom, featureId,sft);
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

        } else if (geom instanceof GeometryCollection) {
            GeometryCollection circle = (GeometryCollection) geom;
            result = GeometryCollectionFeature.createFeature(simpleFeature);
        } else {
            throw new IllegalArgumentException("Unsupported geometry:" + geom.getClass().toString());
        }

        result.setGeomData(geom.getUserData());
        return result;
    }


    private static SimpleFeature createSimpleFeature(Geometry geometry, String id,SimpleFeatureType sft) {

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

    private static SimpleFeature createSimpleFeature(Geometry geometry,SimpleFeatureType sft) {

        return createSimpleFeature(geometry, UUID.randomUUID().toString(),sft);
    }
}

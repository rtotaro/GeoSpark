package org.datasyslab.geospark.simpleFeatureObjects;

import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.type.GeometryDescriptorImpl;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.GeometryDescriptor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class GeometryCollectionFeature<T extends GeometryCollection> extends GeometryFeature<T>  {

    private GeometryCollectionFeature(SimpleFeature delegate) {
        super(delegate);
    }

    public static <T extends GeometryCollection> GeometryCollectionFeature<T> createFeature(SimpleFeature feature) {
        Object defaultGeometry = feature.getDefaultGeometry();
        if (defaultGeometry instanceof GeometryCollection) {
            feature.setDefaultGeometry(defaultGeometry);
            return new GeometryCollectionFeature(feature);
        }

        throw new IllegalArgumentException("default geometry in feature is not a LineString");

    }

    public List<GeometryFeature<T>> toSingleFeatures() {
        T defaultGeometry = this.getDefaultGeometry();
        SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
        simpleFeatureTypeBuilder.init(this.getFeatureType());
        simpleFeatureTypeBuilder.remove(getFeatureType().getGeometryDescriptor().getLocalName());
        simpleFeatureTypeBuilder.add(getFeatureType().getGeometryDescriptor().getLocalName(),defaultGeometry.getGeometryN(0).getClass());
        SimpleFeatureType simpleFeatureType = simpleFeatureTypeBuilder.buildFeatureType();

        List<GeometryFeature<T>> result = new ArrayList<>();
        for (int i = 0; i < defaultGeometry.getNumGeometries(); i++) {
            Geometry geometryN = defaultGeometry.getGeometryN(i);
            GeometryFeature<T> geometryFeature = GeometryFeatureFactory.createGeometryFeature(geometryN,simpleFeatureType);
            geometryFeature.getUserData().putAll(this.getUserData());
            result.add(geometryFeature);
        }
        return result;
    }


}

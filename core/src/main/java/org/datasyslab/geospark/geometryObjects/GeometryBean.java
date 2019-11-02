package org.datasyslab.geospark.geometryObjects;

import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;

public class GeometryBean<T extends Geometry, P extends Serializable> implements Serializable {

    private T geometry;

    private P data;

    public GeometryBean(T geometry, P data) {
        this.geometry = geometry;
        //to avoid recursive serialization
        this.geometry.setUserData(null);
        this.data = data;
    }

    public static  <T extends Geometry, P extends Serializable> GeometryBean of(T geometry, P data)
    {
        return new GeometryBean(geometry,data);
    }
    public static  <T extends Geometry, P extends Serializable> GeometryBean of(T geometry)
    {
        return new GeometryBean(geometry,null);
    }

    public T getGeometry() {
        return geometry;
    }

    public P getData() {
        return data;
    }


}
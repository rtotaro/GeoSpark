/*
 * FILE: CircleRDD
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datasyslab.geospark.spatialRDD;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.simpleFeatureObjects.*;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

// TODO: Auto-generated Javadoc

/**
 * The Class CircleRDD.
 */
public class CircleRDD
        extends SpatialRDD<CircleFeature>
{

    /**
     * Instantiates a new circle RDD.
     *
     * @param circleRDD the circle RDD
     */
    public CircleRDD(JavaRDD<CircleFeature> circleRDD)
    {
        this.rawSpatialRDD = circleRDD;
    }

    /**
     * Instantiates a new circle RDD.
     *
     * @param circleRDD the circle RDD
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCRSCode the target epsg CRS code
     */
    public CircleRDD(JavaRDD<CircleFeature> circleRDD, String sourceEpsgCRSCode, String targetEpsgCRSCode)
    {
        this.rawSpatialRDD = circleRDD;
        this.CRSTransform(sourceEpsgCRSCode, targetEpsgCRSCode);
    }

    /**
     * Instantiates a new circle RDD.
     *
     * @param spatialRDD the spatial RDD
     * @param Radius the radius
     */
    public CircleRDD(SpatialRDD spatialRDD, Double Radius)
    {
        final Double radius = Radius;
        this.rawSpatialRDD = spatialRDD.rawSpatialRDD.map(new Function<GeometryFeature, CircleFeature>()
        {

            public CircleFeature call(GeometryFeature v1)
            {
                //TODO:use radius
                return (CircleFeature) GeometryFeature.createGeometryFeature(v1);
            }
        });
        this.CRStransformation = spatialRDD.CRStransformation;
        this.sourceEpsgCode = spatialRDD.sourceEpsgCode;
        this.targetEpgsgCode = spatialRDD.targetEpgsgCode;
    }

    /**
     * Gets the center point as spatial RDD.
     *
     * @return the center point as spatial RDD
     */
    public PointRDD getCenterPointAsSpatialRDD()
    {
        return new PointRDD(this.rawSpatialRDD.map(new Function<CircleFeature, PointFeature>()
        {

            public PointFeature call(CircleFeature circle)
            {
                return PointFeature.createFeature(circle, (Point)circle.getDefaultGeometry().getCenterGeometry());
            }
        }));
    }

    /**
     * Gets the center polygon as spatial RDD.
     *
     * @return the center polygon as spatial RDD
     */
    public PolygonRDD getCenterPolygonAsSpatialRDD()
    {
        return new PolygonRDD(this.rawSpatialRDD.map(new Function<CircleFeature, PolygonFeature>()
        {

            public PolygonFeature call(CircleFeature circle)
            {
                return PolygonFeature.createFeature(circle,(Polygon) circle.getDefaultGeometry().getCenterGeometry());
            }
        }));
    }

    /**
     * Gets the center line string RDD as spatial RDD.
     *
     * @return the center line string RDD as spatial RDD
     */
    public LineStringRDD getCenterLineStringRDDAsSpatialRDD()
    {
        return new LineStringRDD(this.rawSpatialRDD.map(new Function<CircleFeature, LineStringFeature>()
        {

            public LineStringFeature call(CircleFeature circle)
            {

                return LineStringFeature.createFeature(circle,(LineString) circle.getDefaultGeometry().getCenterGeometry());
            }
        }));
    }

    /**
     * Gets the center rectangle RDD as spatial RDD.
     *
     * @return the center rectangle RDD as spatial RDD
     */
    public RectangleRDD getCenterRectangleRDDAsSpatialRDD()
    {
        return new RectangleRDD(this.rawSpatialRDD.map(new Function<CircleFeature, PolygonFeature>()
        {

            public PolygonFeature call(CircleFeature circle)
            {
                return PolygonFeature.createFeature(circle,(Polygon) circle.getDefaultGeometry().getCenterGeometry());
            }
        }));
    }
}

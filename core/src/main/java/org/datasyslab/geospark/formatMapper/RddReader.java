/*
 * FILE: RddReader
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
package org.datasyslab.geospark.formatMapper;

import org.apache.spark.api.java.JavaRDD;
import org.datasyslab.geospark.simpleFeatureObjects.GeometryFeature;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.locationtech.jts.geom.Geometry;

class RddReader
{
    public static SpatialRDD<GeometryFeature> createSpatialRDD(JavaRDD rawTextRDD, FormatMapper<GeometryFeature> formatMapper)
    {
        SpatialRDD spatialRDD = new SpatialRDD<GeometryFeature>();
        spatialRDD.rawSpatialRDD = rawTextRDD.mapPartitions(formatMapper);
        spatialRDD.fieldNames = formatMapper.readPropertyNames(rawTextRDD.take(1).get(0).toString());
        return spatialRDD;
    }
}

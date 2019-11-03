/*
 * FILE: RangeFilterUsingIndex
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
package org.datasyslab.geospark.rangeJudgement;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.datasyslab.geospark.simpleFeatureObjects.GeometryFeature;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// TODO: Auto-generated Javadoc

public class RangeFilterUsingIndex<U extends Geometry, T extends Geometry>
        extends JudgementBase
        implements FlatMapFunction<Iterator<SpatialIndex>, GeometryFeature<T>>
{

    public RangeFilterUsingIndex(U queryWindow, boolean considerBoundaryIntersection, boolean leftCoveredByRight)
    {
        super(queryWindow, considerBoundaryIntersection, leftCoveredByRight);
    }

    /**
     * Call.
     *
     * @param treeIndexes the tree indexes
     * @return the iterator
     * @throws Exception the exception
     */
    /* (non-Javadoc)
     * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
     */
    @Override
    public Iterator<GeometryFeature<T>> call(Iterator<SpatialIndex> treeIndexes)
            throws Exception
    {
        assert treeIndexes.hasNext() == true;
        SpatialIndex treeIndex = treeIndexes.next();
        List<GeometryFeature<T>> results = new ArrayList<GeometryFeature<T>>();
        List<GeometryFeature<T>> tempResults = treeIndex.query(this.queryGeometry.getEnvelopeInternal());
        for (GeometryFeature<T> tempResult : tempResults) {
            if (leftCoveredByRight) {
                if (match(tempResult.getDefaultGeometry(), queryGeometry)) {
                    results.add(tempResult);
                }
            }
            else {
                if (match(queryGeometry, tempResult.getDefaultGeometry())) {
                    results.add(tempResult);
                }
            }
        }
        return results.iterator();
    }
}

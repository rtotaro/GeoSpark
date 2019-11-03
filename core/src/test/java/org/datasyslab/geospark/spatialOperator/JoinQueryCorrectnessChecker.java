/*
 * FILE: JoinQueryCorrectnessChecker
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.datasyslab.geospark.spatialOperator;

import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.simpleFeatureObjects.GeometryFeature;
import org.datasyslab.geospark.simpleFeatureObjects.LineStringFeature;
import org.datasyslab.geospark.simpleFeatureObjects.PointFeature;
import org.datasyslab.geospark.simpleFeatureObjects.PolygonFeature;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.datasyslab.geospark.GeoSparkTestBase;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialRDD.*;
import org.locationtech.jts.geom.*;
import scala.Tuple2;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class JoinQueryCorrectnessChecker
        extends GeoSparkTestBase
{

    /**
     * The test polygon window set.
     */
    public static List<PolygonFeature> testPolygonWindowSet;

    /**
     * The test inside polygon set.
     */
    public static List<PolygonFeature> testInsidePolygonSet;

    /**
     * The test overlapped polygon set.
     */
    public static List<PolygonFeature> testOverlappedPolygonSet;

    /**
     * The test outside polygon set.
     */
    public static List<PolygonFeature> testOutsidePolygonSet;

    /**
     * The test inside line string set.
     */
    public static List<LineStringFeature> testInsideLineStringSet;

    /**
     * The test overlapped line string set.
     */
    public static List<LineStringFeature> testOverlappedLineStringSet;

    /**
     * The test outside line string set.
     */
    public static List<LineStringFeature> testOutsideLineStringSet;

    /**
     * The test inside point set.
     */
    public static List<PointFeature> testInsidePointSet;

    /**
     * The test on boundary point set.
     */
    public static List<PointFeature> testOnBoundaryPointSet;

    /**
     * The test outside point set.
     */
    public static List<PointFeature> testOutsidePointSet;

    private static final GeometryFactory geometryFactory = new GeometryFactory();

    private final GridType gridType;

    public JoinQueryCorrectnessChecker(GridType gridType)
    {
        this.gridType = gridType;
    }

    @Parameterized.Parameters
    public static Collection testParams()
    {
        return Arrays.asList(new Object[][] {
                {GridType.RTREE},
                {GridType.QUADTREE},
                {GridType.KDBTREE},
        });
    }

    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        GeoSparkTestBase.initialize(JoinQueryCorrectnessChecker.class.getSimpleName());

        // Define the user data saved in window objects and data objects
        testPolygonWindowSet = new ArrayList<>();
        testInsidePolygonSet = new ArrayList<>();
        testOverlappedPolygonSet = new ArrayList<>();
        testOutsidePolygonSet = new ArrayList<>();

        testInsideLineStringSet = new ArrayList<>();
        testOverlappedLineStringSet = new ArrayList<>();
        testOutsideLineStringSet = new ArrayList<>();

        testInsidePointSet = new ArrayList<>();
        testOnBoundaryPointSet = new ArrayList<>();
        testOutsidePointSet = new ArrayList<>();

        // Generate all test data

        // Generate test windows. Each window is a 5 by 5 rectangle-style polygon.
        // All data is uniformly distributed in a 10 by 10 window space.

        for (int baseX = 0; baseX < 100; baseX += 10) {
            for (int baseY = 0; baseY < 100; baseY += 10) {
                String id = baseX + ":" + baseY;
                String a = "a:" + id;
                String b = "b:" + id;

                testPolygonWindowSet.add((PolygonFeature) wrap(makeSquare(baseX, baseY, 5), a));
                testPolygonWindowSet.add((PolygonFeature)wrap(makeSquare(baseX, baseY, 5), b));

                // Polygons
                testInsidePolygonSet.add((PolygonFeature)wrap(makeSquare(baseX + 2, baseY + 2, 2), a));
                testInsidePolygonSet.add((PolygonFeature)wrap(makeSquare(baseX + 2, baseY + 2, 2), b));

                testOverlappedPolygonSet.add((PolygonFeature)wrap(makeSquare(baseX + 3, baseY + 3, 3), a));
                testOverlappedPolygonSet.add((PolygonFeature)wrap(makeSquare(baseX + 3, baseY + 3, 3), b));

                testOutsidePolygonSet.add((PolygonFeature)wrap(makeSquare(baseX + 6, baseY + 6, 3), a));
                testOutsidePolygonSet.add((PolygonFeature)wrap(makeSquare(baseX + 6, baseY + 6, 3), b));

                // LineStrings
                testInsideLineStringSet.add((LineStringFeature) wrap(makeSquareLine(baseX + 2, baseY + 2, 2), a));
                testInsideLineStringSet.add((LineStringFeature)wrap(makeSquareLine(baseX + 2, baseY + 2, 2), b));

                testOverlappedLineStringSet.add((LineStringFeature)wrap(makeSquareLine(baseX + 3, baseY + 3, 3), a));
                testOverlappedLineStringSet.add((LineStringFeature)wrap(makeSquareLine(baseX + 3, baseY + 3, 3), b));

                testOutsideLineStringSet.add((LineStringFeature)wrap(makeSquareLine(baseX + 6, baseY + 6, 3), a));
                testOutsideLineStringSet.add((LineStringFeature)wrap(makeSquareLine(baseX + 6, baseY + 6, 3), b));

                // Points
                testInsidePointSet.add((PointFeature)wrap(makePoint(baseX + 2.5, baseY + 2.5), a));
                testInsidePointSet.add((PointFeature)wrap(makePoint(baseX + 2.5, baseY + 2.5), b));

                testOnBoundaryPointSet.add((PointFeature)wrap(makePoint(baseX + 5, baseY + 5), a));
                testOnBoundaryPointSet.add((PointFeature)wrap(makePoint(baseX + 5, baseY + 5), b));

                testOutsidePointSet.add((PointFeature)wrap(makePoint(baseX + 6, baseY + 6), a));
                testOutsidePointSet.add((PointFeature)wrap(makePoint(baseX + 6, baseY + 6), b));
            }
        }
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown()
    {
        sc.stop();
    }

    private static Polygon makeSquare(double minX, double minY, double side)
    {
        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(minX, minY);
        coordinates[1] = new Coordinate(minX + side, minY);
        coordinates[2] = new Coordinate(minX + side, minY + side);
        coordinates[3] = new Coordinate(minX, minY + side);
        coordinates[4] = coordinates[0];

        return geometryFactory.createPolygon(coordinates);
    }

    private static LineString makeSquareLine(double minX, double minY, double side)
    {
        Coordinate[] coordinates = new Coordinate[3];
        coordinates[0] = new Coordinate(minX, minY);
        coordinates[1] = new Coordinate(minX + side, minY);
        coordinates[2] = new Coordinate(minX + side, minY + side);

        return geometryFactory.createLineString(coordinates);
    }

    private static Point makePoint(double x, double y)
    {
        return geometryFactory.createPoint(new Coordinate(x, y));
    }

    private static <T extends Geometry> GeometryFeature<T> wrap(T geometry, Object userData)
    {
        geometry.setUserData(userData);
        return GeometryFeature.createGeometryFeature(geometry);
    }

    private <T extends GeometryFeature, U extends GeometryFeature> void prepareRDDs(SpatialRDD<T> objectRDD,
            SpatialRDD<U> windowRDD)
            throws Exception
    {
        objectRDD.rawSpatialRDD.repartition(4);
        objectRDD.spatialPartitioning(gridType);
        objectRDD.buildIndex(IndexType.RTREE, true);
        windowRDD.spatialPartitioning(objectRDD.getPartitioner());
    }

    /**
     * Test inside point join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testInsidePointJoinCorrectness()
            throws Exception
    {
        PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        PointRDD objectRDD = new PointRDD(sc.parallelize(this.testInsidePointSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<PolygonFeature, HashSet<PointFeature>>> result = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true, false).collect();
        verifyJoinResults(result);

        List<Tuple2<PolygonFeature, HashSet<PointFeature>>> resultNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false, false).collect();
        verifyJoinResults(resultNoIndex);
    }

    /**
     * Test on boundary point join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testOnBoundaryPointJoinCorrectness()
            throws Exception
    {
        PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        PointRDD objectRDD = new PointRDD(sc.parallelize(this.testOnBoundaryPointSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<PolygonFeature, HashSet<PointFeature>>> result = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true, false).collect();
        verifyJoinResults(result);

        List<Tuple2<PolygonFeature, HashSet<PointFeature>>> resultNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false, false).collect();
        verifyJoinResults(resultNoIndex);
    }

    /**
     * Test outside point join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testOutsidePointJoinCorrectness()
            throws Exception
    {
        PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        PointRDD objectRDD = new PointRDD(sc.parallelize(this.testOutsidePointSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<PolygonFeature, HashSet<PointFeature>>> result = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true, false).collect();
        assertEquals(0, result.size());

        List<Tuple2<PolygonFeature, HashSet<PointFeature>>> resultNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false, false).collect();
        assertEquals(0, resultNoIndex.size());
    }

    /**
     * Test inside line string join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testInsideLineStringJoinCorrectness()
            throws Exception
    {
        PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        LineStringRDD objectRDD = new LineStringRDD(sc.parallelize(this.testInsideLineStringSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<PolygonFeature, HashSet<LineStringFeature>>> result = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true, false).collect();
        verifyJoinResults(result);

        List<Tuple2<PolygonFeature, HashSet<LineStringFeature>>> resultNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false, false).collect();
        verifyJoinResults(resultNoIndex);
    }

    /**
     * Test overlapped line string join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testOverlappedLineStringJoinCorrectness()
            throws Exception
    {
        PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        LineStringRDD objectRDD = new LineStringRDD(sc.parallelize(this.testOverlappedLineStringSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<PolygonFeature, HashSet<LineStringFeature>>> result = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true, true).collect();
        verifyJoinResults(result);

        List<Tuple2<PolygonFeature, HashSet<LineStringFeature>>> resultNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false, true).collect();
        verifyJoinResults(resultNoIndex);
    }

    /**
     * Test outside line string join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testOutsideLineStringJoinCorrectness()
            throws Exception
    {
        PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        LineStringRDD objectRDD = new LineStringRDD(sc.parallelize(this.testOutsideLineStringSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<PolygonFeature, HashSet<LineStringFeature>>> result = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true, false).collect();
        assertEquals(0, result.size());

        List<Tuple2<PolygonFeature, HashSet<LineStringFeature>>> resultNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false, false).collect();
        assertEquals(0, resultNoIndex.size());
    }

    /**
     * Test inside polygon join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testInsidePolygonJoinCorrectness()
            throws Exception
    {
        PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        PolygonRDD objectRDD = new PolygonRDD(sc.parallelize(this.testInsidePolygonSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<PolygonFeature, HashSet<PolygonFeature>>> result = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true, false).collect();
        verifyJoinResults(result);

        List<Tuple2<PolygonFeature, HashSet<PolygonFeature>>> resultNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false, false).collect();
        verifyJoinResults(resultNoIndex);
    }

    /**
     * Test overlapped polygon join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testOverlappedPolygonJoinCorrectness()
            throws Exception
    {
        PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        PolygonRDD objectRDD = new PolygonRDD(sc.parallelize(this.testOverlappedPolygonSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<PolygonFeature, HashSet<PolygonFeature>>> result = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true, true).collect();
        verifyJoinResults(result);

        List<Tuple2<PolygonFeature, HashSet<PolygonFeature>>> resultNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false, true).collect();
        verifyJoinResults(resultNoIndex);
    }

    private <U extends GeometryFeature, T extends GeometryFeature> void verifyJoinResults(List<Tuple2<U, HashSet<T>>> result)
    {
        assertEquals(200, result.size());
        for (Tuple2<U, HashSet<T>> tuple : result) {
            U window = tuple._1;
            Set<T> objects = tuple._2;
            String windowUserData = (String) window.getGeomData();

            String[] tokens = windowUserData.split(":", 2);
            String prefix = tokens[0];
            String id = tokens[1];

            assertTrue(prefix.equals("a") || prefix.equals("b"));
            assertEquals(2, objects.size());

            final Set<String> objectIds = new HashSet<>();
            for (T object : objects) {
                objectIds.add((String) object.getGeomData());
            }

            assertEquals(new HashSet(Arrays.asList("a:" + id, "b:" + id)), objectIds);
        }
    }

    /**
     * Test outside polygon join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testOutsidePolygonJoinCorrectness()
            throws Exception
    {
        PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        PolygonRDD objectRDD = new PolygonRDD(sc.parallelize(this.testOutsidePolygonSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<PolygonFeature, HashSet<PolygonFeature>>> result = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true, false).collect();
        assertEquals(0, result.size());

        List<Tuple2<PolygonFeature, HashSet<PolygonFeature>>> resultNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false, false).collect();
        assertEquals(0, result.size());
    }

    /**
     * Test inside polygon distance join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testInsidePolygonDistanceJoinCorrectness()
            throws Exception
    {
        PolygonRDD centerGeometryRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        CircleRDD windowRDD = new CircleRDD(centerGeometryRDD, 0.1);
        PolygonRDD objectRDD = new PolygonRDD(sc.parallelize(this.testInsidePolygonSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<GeometryFeature, HashSet<PolygonFeature>>> result = JoinQuery.DistanceJoinQuery(objectRDD, windowRDD, true, false).collect();
        verifyJoinResults(result);

        List<Tuple2<GeometryFeature, HashSet<PolygonFeature>>> resultNoIndex = JoinQuery.DistanceJoinQuery(objectRDD, windowRDD, false, false).collect();
        verifyJoinResults(resultNoIndex);
    }

    /**
     * Test overlapped polygon distance join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testOverlappedPolygonDistanceJoinCorrectness()
            throws Exception
    {
        PolygonRDD centerGeometryRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        CircleRDD windowRDD = new CircleRDD(centerGeometryRDD, 0.1);
        PolygonRDD objectRDD = new PolygonRDD(sc.parallelize(this.testOverlappedPolygonSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<GeometryFeature, HashSet<PolygonFeature>>> result = JoinQuery.DistanceJoinQuery(objectRDD, windowRDD, true, true).collect();
        verifyJoinResults(result);

        List<Tuple2<GeometryFeature, HashSet<PolygonFeature>>> resultNoIndex = JoinQuery.DistanceJoinQuery(objectRDD, windowRDD, false, true).collect();
        verifyJoinResults(resultNoIndex);
    }

    /**
     * Test outside polygon distance join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testOutsidePolygonDistanceJoinCorrectness()
            throws Exception
    {
        PolygonRDD centerGeometryRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY());
        CircleRDD windowRDD = new CircleRDD(centerGeometryRDD, 0.1);
        PolygonRDD objectRDD = new PolygonRDD(sc.parallelize(this.testOutsidePolygonSet), StorageLevel.MEMORY_ONLY());
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<GeometryFeature, HashSet<PolygonFeature>>> result = JoinQuery.DistanceJoinQuery(objectRDD, windowRDD, true, true).collect();
        assertEquals(0, result.size());

        List<Tuple2<GeometryFeature, HashSet<PolygonFeature>>> resultNoIndex = JoinQuery.DistanceJoinQuery(objectRDD, windowRDD, false, true).collect();
        assertEquals(0, resultNoIndex.size());
    }
}

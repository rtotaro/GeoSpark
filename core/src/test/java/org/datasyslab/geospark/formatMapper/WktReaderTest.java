package org.datasyslab.geospark.formatMapper;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.datasyslab.geospark.GeoSparkTestBase;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class WktReaderTest extends GeoSparkTestBase {

    public static String wktGeometries = null;

    @BeforeClass
    public static void onceExecutedBeforeAll()
            throws IOException
    {
        initialize(WktReaderTest.class.getName());
        wktGeometries = WktReaderTest.class.getClassLoader().getResource("county_small.tsv").getPath();
    }

    @AfterClass
    public static void tearDown()
            throws Exception
    {
        sc.stop();
    }

    /**
     * Test correctness of parsing geojson file
     *
     * @throws IOException
     */
    @Test
    public void testReadToGeometryRDD()
            throws IOException
    {
        // load geojson with our tool
        SpatialRDD wktRDD = WktReader.readToGeometryRDD(sc, wktGeometries, 0, true, false);
        assertEquals(wktRDD.rawSpatialRDD.count(), 103);
    }
}

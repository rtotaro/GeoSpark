package org.locationtech.geomesa.spark

/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

import java.util.concurrent.ConcurrentHashMap

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.datasyslab.geospark.simpleFeatureObjects.{CircleFeature, GeometryFeature, LineStringFeature, PointFeature, PolygonFeature, RectangleFeature}
import org.geotools.data.DataStore
import org.geotools.feature.simple.SimpleFeatureImpl
import org.locationtech.geomesa.features.SimpleFeatureSerializers
import org.locationtech.geomesa.features.kryo.serialization.SimpleFeatureSerializer
import org.locationtech.geomesa.utils.cache.CacheKeyGenerator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.util.hashing.MurmurHash3

class GeoMesaSparkKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    val serializer = new com.esotericsoftware.kryo.Serializer[SimpleFeature]() {
      val cache = new ConcurrentHashMap[Int, SimpleFeatureSerializer]()

      override def write(kryo: Kryo, out: Output, feature: SimpleFeature): Unit = {
        var serializer = GeometryFeature.serializer
        out.writeInt(GeometryFeature.geometryFeatureType.hashCode(), true)
        serializer.write(kryo, out, feature)
      }

      override def read(kryo: Kryo, in: Input, clazz: Class[SimpleFeature]): SimpleFeature = {
        val id = in.readInt(true)
        var serializer = GeometryFeature.serializer
        GeometryFeature.createGeometryFeature(serializer.read(kryo, in, clazz))
      }
    }
//    kryo.setReferences(false)
//    SimpleFeatureSerializers.simpleFeatureImpls.foreach(kryo.register(_, serializer, kryo.getNextRegistrationId))
    kryo.register(classOf[GeometryFeature[_]], serializer, kryo.getNextRegistrationId)
    kryo.register(classOf[SimpleFeatureImpl], serializer, kryo.getNextRegistrationId)
    kryo.register(classOf[PointFeature], serializer, kryo.getNextRegistrationId)
    kryo.register(classOf[PolygonFeature], serializer, kryo.getNextRegistrationId)
    kryo.register(classOf[LineStringFeature], serializer, kryo.getNextRegistrationId)
    kryo.register(classOf[CircleFeature], serializer, kryo.getNextRegistrationId)
    kryo.register(classOf[RectangleFeature], serializer, kryo.getNextRegistrationId)

  }
}





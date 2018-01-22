/*
 * Copyright 2018 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.avro.types

import org.apache.avro.Schema
import org.scalatest._
class DebugTest extends FlatSpec with Matchers {

  @AvroType.fromSchema(
    """
      |{
      |  "name": "A", "namespace": "outer",
      |  "type": "record", "fields": [
      |    {
      |      "name": "a",
      |      "type": {
      |        "type": "record",
      |        "name": "A", "namespace": "inner",
      |        "fields": [{"name": "intF", "type": "int"}]
      |      }
      |    }
      |  ]
      |}
    """.stripMargin)
  class SameName

  it should "support nested record with same name as enclosing record" in {
    println(SameName.schema.toString(true))
    val r = SameName(SameName$A(1))
    SameName.toGenericRecord(r)
//    println(SameName$A)
//    val schema2: Schema = SameName.toGenericRecord(r).getSchema
//    println(schema2.toString(true))
//    SameName.toGenericRecord(r).getSchema.toString
    true
  }

}

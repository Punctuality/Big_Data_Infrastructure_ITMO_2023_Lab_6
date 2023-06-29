package com.github.Punctuality.model

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types.StructType
import org.json4s._

import scala.reflect.ClassTag

case class SugarEnergy(productName: String, sugar: Double, energy: Int) {
  def toTuple: (String, Double, Int) = (productName, sugar, energy)
}

object SugarEnergy {
  private  implicit val formats: Formats = DefaultFormats

  implicit val jReader: Reader[SugarEnergy] = (value: JValue) => {
    (for {
      name <- (value \ "product_name").extractOpt[String]
      sugar <- (value \ "nutriments" \ "sugars_100g").extractOpt[Double]
      energy <- (value \ "nutriments" \ "energy_100g").extractOpt[Int]
    } yield SugarEnergy(name, sugar, energy))
      //        Shitty design, but we have to deal with it...
      .getOrElse(throw new Exception("Failed to parse SugarEnergy"))
  }

//  implicit val sparkEncoder: Encoder[SugarEnergy] = new Encoder[SugarEnergy] {
//    override def schema: StructType = StructType.fromDDL("name STRING, sugar DOUBLE, energy INT")
//
//    override def clsTag: ClassTag[SugarEnergy] = ClassTag(SugarEnergy.getClass)
//  }
}
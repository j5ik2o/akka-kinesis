package com.github.j5ik2o.ak.persistence

package object serialization {

  def encodeTags(tags: Set[String], tagSeparator: String): Option[String] =
    if (tags.isEmpty) None else Option(tags.mkString(tagSeparator))

  def decodeTags(tags: Option[String], tagSeparator: String): Set[String] =
    tags.map(_.split(tagSeparator).toSet).getOrElse(Set.empty[String])

}

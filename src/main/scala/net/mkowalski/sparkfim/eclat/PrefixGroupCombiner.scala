package net.mkowalski.sparkfim.eclat

import net.mkowalski.sparkfim.model.{MinSupport, PrefixGroupExtension}

abstract class PrefixGroupCombiner(val minSupport: MinSupport) {

  def combine(extension: PrefixGroupExtension, another: PrefixGroupExtension): Iterable[PrefixGroupExtension]

}

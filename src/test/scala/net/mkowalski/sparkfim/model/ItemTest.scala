package net.mkowalski.sparkfim.model

import net.mkowalski.sparkfim.test.BaseTest

class ItemTest extends BaseTest {

  test("Items with same prefix") {
    val item1 = Item(1, 2, 3, 4)
    val item2 = Item(1, 2, 3, 5)
    assert(item1.prefix == item2.prefix)
  }

  test("Empty prefix when single item id") {
    val item = Item(1)
    val emptyPrefix = Prefix()
    assert(item.prefix == emptyPrefix)
  }

  test("Valid prefix when multiple item ids") {
    val item = Item(1, 2, 3, 4, 5)
    val expectedPrefix = Prefix(1, 2, 3, 4)
    assert(item.prefix == expectedPrefix)
  }

  test("Single item extension when single item id") {
    val item = Item(1)
    assert(item.extension == 1)
  }

  test("Valid item extension when multiple item ids") {
    val item = Item(1, 2, 3)
    assert(item.extension == 3)
  }

  test("Empty items equality") {
    assert(Item() == Item())
  }

  test("Non empty items equality") {
    assert(Item(1, 2, 3) == Item(1, 2, 3))
  }

  test("Empty items hashcode equality") {
    // by default arrays (different objects) with same elements will get different hashCode
    // so this check ensures correctness of the hashCode() method
    assert(Item().hashCode() == Item().hashCode())
  }

  test("Non empty, single id items hashcode equality") {
    assert(Item(1, 2).hashCode() == Item(1, 2).hashCode())
  }




  test("Can mergeOption items with same prefix") {
    val item1 = Item(1, 2, 3, 4)
    val item2 = Item(1, 2, 3, 5)
    assertResult(true) {
      item1.merge(item2).isDefined
    }
  }

  test("Can mergeOption items with empty prefix") {
    val item1 = Item(1)
    val item2 = Item(2)
    assertResult(true) {
      item1.merge(item2).isDefined
    }
  }

  test("Can't mergeOption items with different prefix") {
    val item1 = Item(1, 2)
    val item2 = Item(2)
    assertResult(false) {
      item1.merge(item2).isDefined
    }
  }

}

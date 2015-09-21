# Spark-FIM

Spark-FIM provides Frequent Itemsets Mining algorithms implemented for Apache Spark platform (using Scala). 
Currently the following algorithms are available:

* _DistEclat_ (_Moens, Sandy, Emin Aksehirli, and Bart Goethals. "Frequent itemset mining for big data." Big Data, 
2013 IEEE International Conference on. IEEE, 2013_ - MapReduce implementation available [here](https://gitlab.com/adrem/bigfim-sa))
* _BigFIM_ (published & distributed with _DistEclat_).


### Building project

Since the project uses [sbt](http://www.scala-sbt.org/) as a build tool, to prepare _.jar_ package run:

    sbt package
    
    
### Running tests

Spark provides a great way to simulate distributed environment on the local computer. To run all the tests (including Spark running in local mode) run:

	sbt test
    
### Running algorithms

All available algorithms can be customized easily using command line arguments. To see available parameters run driver class without arguments:

	# DistEclat
    spark-submit --class "net.mkowalski.sparkfim.driver.DistEclatDriver"
    
    # BigFIM
    spark-submit --class "net.mkowalski.sparkfim.driver.BigFimDriver"

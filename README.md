# pyspark-parsers

Collection of PySpark-based parsing scripts to handle with
different datafile types

## Installing dependencies

- At first, you need to install Spark infrastructure 
(suppose you already have ```conda``` package manager)
```shell script
conda install -c conda-forge pyspark 
sudo add-apt-repository ppa:openjdk-r/ppa
sudo apt install openjdk-8-jdk openjdk-8-jre
curl -O https://www-us.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz
tar xvf spark-2.4.4-bin-hadoop2.7.tgz
sudo mv spark-2.4.4-bin-hadoop2.7/ /opt/spark 
```

## Sample data

All sample datasets are located at data/ directory

- ```sales.csv ``` - 10000 records of "sales data" 
generated through random logic in VBA 
([link](http://eforexcel.com/wp/downloads-18-sample-csv-files-data-sets-for-testing-sales/))

- ```menu.xml``` - short breakfast menu presented as an xml-file 
([link](http://www-db.deis.unibo.it/courses/TW/DOCS/w3schools/xml/xml_examples.asp.html))
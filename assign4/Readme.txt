The goal of this assignment is to familiarize yourself with:

defining objects using AVRO
generating the Java bindings
building/using/referencing AVRO objects in your map-reduce code
understanding output of AVRO defined objects
In this assignment you will:

define an object (using AVRO) that contains the data produced by the WordStatistics app:
document count
total count
min
max
sum of squares
mean
variance
Call this AVRO defined object WordStatisticsData
Modify your WordStatistics app from Assignment 2 to use WordStatisticsData, and collect stats for words and paragraph length, including min and max (word count in a paragraph, or paragraph length)
Use the example code from WordCountA.java as your guide for using an AVRO defined object. This file is available on Canvas (Files / Assignment 4).

For maven to see your AVRO schema file (file suffix is:  .avsc), you need to place it under the directory: src/main/avro

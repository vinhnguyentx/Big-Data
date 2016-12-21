For assignment 2, the task is to compute the mean and variance for each word that appears in one or more paragraphs of the input document. Each line in the input file represents one paragraph.

Input file: s3://utcs378/data/dataSet2.txt
Output file content: Document count, mean, and variance for the words that appear in the document.
Output format: Text file with fields: word, \t, paragraph (document) count, mean, variance.
Required elements:

Name your class WordStatistics.
Implement a custom class that implements the Writable interface (name this class WordStatisticsWritable), and use this class for map output, and reducer input and output.
Implement and use a combiner.
Remove punctuation (commas, periods, double quotes, ....) and convert the text to lowercase. I suggest that you first run the WordCount program on dataSet2.txt to see what punctuation exists in the document. This document contains footnote citations like this:  "[174]".  Consider these as words.

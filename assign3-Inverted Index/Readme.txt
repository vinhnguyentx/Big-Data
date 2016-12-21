Created an inverted index as described in class (and in the textbook), where the output is a file of key/value pairs, with the key being a word, and the value being a list of verses in which that word appears.

For this assignment you will need to extract the verse ID at the beginning of each line.
Format of the ID is:
     book:chapter:verse
Where book is a string, and chapter and verse are integers. The content of the verse is the remainder of the input line (separated from the ID by whitespace). You should remove punctuation and make all the words lower cas

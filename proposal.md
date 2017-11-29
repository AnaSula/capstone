## Capstone Project Proposal


### Abstract

The purpose of this project is to develop an automated solution for identifying relationships between documents by capturing textual references to other documents in a document body. The captured reference then will be matched to the relevant documents and will be connected using entity reference module in Drupal content management system.


### Data

The data used for this project consists of two parts: 
38 documents, specifically  Department of Labor Field Assistant Bulletins, queried from Solr index which will be used to identify the textual references to other documents; 
titles and unique IDs of 113,097 documents queried from MySQL database which will be used to match the extracted references. The data will not be split in test and validation sets.

### Process

The process of the project will develop in three major steps: 
querying the data from the mentioned sources, using python module and SQL; 
writing a python module to extract textual references to other documents based on various patterns, using regular expressions; 
writing a python module to match extracted list of strings to the titles of documents queried from MySQL database. The last module will contain multiple classes that will separately process queried titles to match the key terms/words to the extracted strings.

### Final Product

The final product will be a csv file containing pairs of document titles and IDs (a pair per row), indicating that the documents in a pair are related. Python pandas module will be used to make the file. Finally, the csv file will be consumed by Drupal’s entity reference module. The screenshot of final outcome below:

![https://github.com/AnaSula/capstone/blob/master/image.png]


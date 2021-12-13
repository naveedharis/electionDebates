# electionDebates #

## Haris Naveed ##
## Data Wrangling ##
## Professor Benjamin Coleman ##

This program takes a directory of csv files to read, modify, add, and store data
into a MYSQL database. The resource that I used is from the Kaggle data website:
https://www.kaggle.com/headsortails/us-election-2020-presidential-debates
Each csv file has a 3 columns: speaker, minute, and text. For this project, I
added a party type, number of words, number of questions, and a true or false
where the candidates talked about the economy, health care, covid, and supreme
court. I had to modify the speaker table because some files they had similar names
like for Joe Biden. There was Former Vice President Joe Biden. For Trump, there were
Donald J. Trump, President Donald Trump, and etc. I used regular expressions to identify
different candidates. Then, put all the data and split into 6 tables: speaker, text,
economy, health care, covid, and supreme court. 


Also, there is a Jupyter notebook where I tested my code and methods. 

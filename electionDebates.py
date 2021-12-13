'''
ElectionDebates
Haris Naveed
Data Wrangling
Professor Benjamin Coleman

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
'''



import pandas as pd
import os
import re
from dotenv import load_dotenv
import mysql.connector
import string

load_dotenv()

republican_candidate = "Trump$"
republican_vice_candidate = "Pence$"
democratic_candidate = "Biden$"
democratic_vice_candidate = "Harris$"
host1 = "Welker$"
host2 = "Stephanopoulos$"
host3 = "Wallace$"
host4 = "Page$"
host5 = "Guthrie$"
race = ["race", "equality", "racial", "racist", "african-american", "mr. floyd", "radical"]
violence = ["radical", "mr. floyd", "racist", "guns", "cops", "protest"]
economy = ["inflation", "economy", "capital", "credit", "economics", "booming", "market",
           "interest", "deflation", "poverty", "jobs", "unemployment", "recession", "incentives",
           "infrastructure", "paying", "reserve"]
health_care = ["health care", "medical", "obamacare", "insurance", "affordable", "care", "act", 'mecaid']
covid = ["covid-19", "covid", "virus", "Xi", "china", "cases"]
supreme_court = ['supreme court', "judge", "case"]
intro = ["hello", "good","well", "how are you", "evening", "beginning", "how are you doing", "end"]



def remove_punct(speaker):
    '''
    This function removes the extra characters and spaces in the speaker.

    Parameter:
    speaker - the person who is speaker - String

    Return:
    speaker - the person who is speaker - String
    '''
    speaker = speaker.strip()
    translator = str.maketrans("", "", string.punctuation)
    return speaker.translate(translator)


def create_database(cursor):
    '''
    This function creates a database called ElectionDebates and drop its
    if it exists.

    Parameter:
    cursor = MYSQL cursor

    Return: None
    '''
    cursor.execute('DROP DATABASE IF EXISTS ElectionDebates;')
    cursor.execute('CREATE DATABASE ElectionDebates;')
    cursor.execute('USE ElectionDebates;')


def speaker_table(cursor):
    '''
    This function creates the speaker table

    Parameter:
    cursor = MYSQL cursor

    Return: None
    '''
    cursor.execute('CREATE TABLE Speaker'
                   '(SpeakerID INT UNIQUE, '
                   'Speaker VARCHAR(300), '
                   'Party_type VARCHAR(300),'
                   'PRIMARY KEY (SpeakerID));')


def text_table(cursor):
    '''
    This function creates the text table

    Parameter:
    cursor = MYSQL cursor

    Return: None
    '''
    cursor.execute('CREATE TABLE Text'
                   '(TextID INT UNIQUE,'
                   'Text VARCHAR(5000),'
                   'Minute VARCHAR(300),'
                   'SpeakerID INT,'
                   'Word_count INT,'
                   'Number_of_questions INT,'
                   'PRIMARY KEY (TextID),'
                   'FOREIGN KEY (SpeakerID) REFERENCES Speaker(SpeakerID));')


def other_table(cursor, other):
    '''
    This function creates the economy,
    covid, health care, and supreme court table

    Parameter:
    cursor = MYSQL cursor

    Return: None
    '''
    cursor.execute(f'CREATE TABLE {other}'
                   f'({other}ID INT UNIQUE,'
                   'Text VARCHAR(5000),'
                   'TextID INT,'
                   'SpeakerID INT,'
                   f'PRIMARY KEY ({other}ID),'
                   'FOREIGN KEY (TextID) REFERENCES Text(TextID),'
                   'FOREIGN KEY (SpeakerID) REFERENCES Speaker(SpeakerID));')


def create_tables(cursor):
    '''
    This function creates the different tables

    Parameter:
    cursor = MYSQL cursor

    Return: None
    '''
    speaker_table(cursor)
    text_table(cursor)
    other_table(cursor, "Economy")
    other_table(cursor, "Health_care")
    other_table(cursor, "Covid")
    other_table(cursor, "Supreme_court")



def insert_speaker_table(cursor, speaker_dict):
    '''
    This function insert a speaker dictionary into the speaker
    table.

    Parameter:
    cursor = MYSQL cursor
    speaker_dict - Dictionary of speaker - Dict

    Return: None
    '''
    columns = 'SpeakerID, Speaker, Party_type'
    for key, values in speaker_dict.items():
        query = f'INSERT INTO Speaker ({columns}) VALUES ({values[0]}, "{key}", "{values[1]}");'
        cursor.execute(query)


def insert_text_table(cursor, text_dict):
    '''
    This function insert a text dictionary into the text
    table.

    Parameter:
    cursor = MYSQL cursor
    text_dict - Dictionary of text - Dict

    Return: None
    '''
    columns = 'TextID, Text, Minute, SpeakerID, Word_count, Number_of_questions'
    for key, values in text_dict.items():
        values = ",".join(str(value) for value in values)
        query = f'INSERT INTO Text ({columns}) VALUES ({key}, {values});'
        cursor.execute(query)


def other_insert_table(cursor, other_dict, table):
    '''
    This function insert a other dictionary into the other
    tables like economy, health care, covid, and supreme
    court.

    Parameter:
    cursor = MYSQL cursor
    text_dict - Dictionary of text - Dict

    Return: None
    '''
    columns = f'{table}ID, Text, TextID, SpeakerID'
    for key, values in other_dict.items():
        values = ",".join(str(value) for value in values)
        query = f'INSERT INTO {table} ({columns}) VALUES ({key}, {values});'
        cursor.execute(query)


def connecting_to_database():
    '''
    This function connects to the database and calling functions to read
    the files, creating the tables, and inserting data into the tables.

    Parameter: None
    Return: None
    '''
    USERNAME = os.getenv('USERNAME')
    PASSWORD = os.getenv('PASSWORD')

    conn = mysql.connector.connect(username=USERNAME, password=PASSWORD, host='localhost')
    cursor = conn.cursor()

    create_database(cursor)
    create_tables(cursor)

    speaker_dict, text_dict, economy_dict, health_care_dict, covid_dict, supreme_court_dict = opening_dir_files()
    insert_speaker_table(cursor, speaker_dict)
    insert_text_table(cursor, text_dict)
    other_insert_table(cursor, economy_dict, "Economy")
    other_insert_table(cursor, health_care_dict, "Health_care")
    other_insert_table(cursor, covid_dict, "Covid")
    other_insert_table(cursor, supreme_court_dict, "Supreme_court")

    conn.commit()
    cursor.close()
    conn.close()

def change_data_type_minute(minute):
    '''
    This function changes the data type of minute
    into string.

    Parametere:
    minute - Datetime

    Return:
    minute - String
    '''
    return str(minute)


def checking_number_of_questions(text):
    '''
    This function checks the numbers of questions in one
    text.

    Parameter:
    text - String

    Return:
    count - INT
    '''
    count = 0
    for char in text:
        if char == "?":
            count += 1
    return count


def word_count_func(text):
    '''
    This function checks the word count in one
    text.

    Parameter:
    text - String

    Return:
    len(words) - INT
    '''
    words = text.split()
    return len(words)


def checking_speaker_party(speaker):
    '''
    This function uses regular expressions to identify the party type
    of the speaker.
    :param speaker: String
    :return String
    '''
    speaker = speaker.strip()
    # Comparing the republican presidential and vice-presidential with the speaker
    if re.search(republican_candidate, speaker) != None or re.search(republican_vice_candidate, speaker) != None:
        return "Republican"
    # Comparing the democratic presidential and vice-presidential with the speaker
    elif re.search(democratic_candidate, speaker) != None or re.search(democratic_vice_candidate, speaker) != None:
        return "Democratic"
    # Comparing the different hosts with the speaker
    elif re.search(host1, speaker) != None or re.search(host2, speaker) != None \
            or re.search(host3, speaker) != None or re.search(host4, speaker) != None \
            or re.search(host5, speaker):
        return "Host"
    # For citizens only
    else:
        return "Citizen"

'''
    This function takes the text and list of summary words to 
    identify what subject is the text is talking about. 
    
    Parameters:
    text - String 
    list_of_summary_words - List
    
    Return:
    res - Boolean
'''
def summary_columns(text, list_of_summary_words):
    res = any(word in text for word in list_of_summary_words)
    return res

'''
    This function opens csv file and calls multiple apply methods to modify
    create columns. 
    
    Parameters:
    file - FileType
    speaker_dict - Dictionary
    text_dict - Dictionary
    economy_dict - Dictionary 
    health_care_dict - Dictionary 
    covid_dict - Dictionary
    supreme_court_dict - Dictionary
    
    Return: None
'''
def opening_csv_file(file, speaker_dict, text_dict, economy_dict, health_care_dict, covid_dict, supreme_court_dict):
    '''
    This function reads the csv file with pandas, and split the columns into individual series. Then, we modify the
    series with multiple different functions with apply function.
    :param file: Filetype
    :param speaker_dict: Dictionary - speakers along with speakerID and party_type
    :param text_dict: Dictionary - text along textID, speakerID,
    :param economy_dict: Dictionary - text, textID, speakerID
    :param health_care_dict: Dictionary - text, textID, speakerID
    :param covid_dict: Dictionary - text, textID, speakerID
    :param supreme_court_dict: Dictionary - text, textID, speakerID
    :return: None
    '''
    data = pd.read_csv(file)
    speaker = data['speaker']
    minute = data['minute']
    text = data['text']

    speaker = speaker.apply(remove_punct)
    updated_speaker = speaker.apply(re_search_party_candidates)
    data['speaker'] = updated_speaker
    speaker_set = set(data['speaker'])

    change_minute = minute.apply(change_data_type_minute)
    data['minute'] = change_minute

    word_count = text.apply(word_count_func)
    party_type = speaker.apply(checking_speaker_party)
    number_of_questions = text.apply(checking_number_of_questions)
    economy_true_or_false = text.apply(summary_columns, args=(economy,))
    health_care_true_or_false = text.apply(summary_columns, args=(health_care,))
    covid_true_or_false = text.apply(summary_columns, args=(covid,))
    supreme_court_true_or_false = text.apply(summary_columns, args=(supreme_court,))

    data = data.copy()
    data['party_type'] = party_type
    data['word_count'] = word_count
    data['number_of_questions'] = number_of_questions
    data['economy'] = economy_true_or_false
    data['health_care'] = health_care_true_or_false
    data['covid'] = covid_true_or_false
    data['supreme_court'] = supreme_court_true_or_false

    speaker_dictionary_func(speaker_set, speaker_dict, data)
    dictionary_func(data,speaker_dict,text_dict, economy_dict, health_care_dict, covid_dict, supreme_court_dict)

def re_search_party_candidates(speaker):
    '''
    This function helps change the speaker's name to be same with regular expressions.
    In this data, two presidential candidates had different names in multiple csv files.
    :param speaker:
    :return: String
    '''
    if re.search(republican_candidate, speaker) is not None:
        return "Donald Trump"
    if re.search(democratic_candidate, speaker) is not None:
        return "Joe Biden"
    else:
        return speaker.strip()

def dictionary_func(data, speaker_dict, text_dict, economy_dict, health_care_dict, covid_dict, supreme_court_dict):
    '''
    This function runs a for loop of the values in data and initialize local variables to connect and add onto
    multiple dictionaries.
    :param data: DataFrame
    :param speaker_dict: Dictionary
    :param text_dict: Dictionary
    :param economy_dict: Dictionary
    :param health_care_dict: Dictionary
    :param covid_dict: Dictionary
    :param supreme_court_dict: Dictionary
    :return: None
    '''
    for value in data.values:
        speaker_from_value = value[0]
        minute = f'"{value[1]}"'
        text = f'"{value[2]}"'
        party_type = f'"{value[3]}"'
        word_count = value[4]
        number_of_questions = value[5]
        economy = value[6]
        health_care = value[7]
        covid = value[8]
        supreme_court = value[9]

        speakerID = [values[0] for speaker, values in speaker_dict.items() if speaker == speaker_from_value][0]
        text_dictionary_func(text_dict, speakerID, text, minute, word_count, number_of_questions)
        textID = [key for key, values in text_dict.items() if text == values[0]][0]

        other_dictionary_func(economy_dict, text, speakerID, textID, economy)
        other_dictionary_func(health_care_dict, text, speakerID, textID, health_care)
        other_dictionary_func(covid_dict, text, speakerID, textID, covid)
        other_dictionary_func(supreme_court_dict, text, speakerID, textID, supreme_court)

def other_dictionary_func(other_dict, text, speakerID, textID, other):
    '''
    This function focuses on the economy, health care, covid, and supreme court dictionaries
    because they are the same key and values.
    :param other_dict: Dictionary
    :param text: String
    :param speakerID: Int
    :param textID: Int
    :param other: String
    :return: None
    '''
    if other:
        other_dict[len(other_dict)+1] = text, textID, speakerID


def text_dictionary_func(text_dict, speakerID, text, minute, word_count, number_of_questions):
    '''
    This function takes 6 parameters to update the text
    dictionary. The text dictionary key is the primary id
    for the table 'text'.

    Parameters:
    text_dict - Dictionary of text - Dict
    speakerID - ID of the speaker - INT
    text - String of the dialogue of the speaker - String
    minute - time frame in hours, minutes, and seconds - String
    word_count - integer of number of words in a text - INT
    number_of_questions - integer of number of questions in a text - INT

    Return: None
'''
    text_dict[len(text_dict)+1] = text, minute, speakerID, word_count, number_of_questions


def speaker_dictionary_func(speaker_set, speaker_dict, data):
    '''
    This function takes 3 parameters to update the speaker
    dictionary. Also, it makes sure that the speaker is not
    repeated in the dictionary.

    Parameters:
    speaker_set - Set of speakers - Set
    speaker_dict - Dictionary of speakers - Dict
    data - DataFrame of the file - DataFrame

    Return: None
'''
    for speaker in speaker_set:
        # Getting the political party of the speaker
        party_type = [value[3] for value in data.values if value[0] == speaker][0]
        # Checks if the speaker is not in the dictionary
        if speaker not in speaker_dict.keys():
            speaker_dict[speaker] = len(speaker_dict)+1, party_type


def opening_dir_files():
    '''
    This function finds the directory called archive where the csv files
    and created multiple dictionaries to add new data.
    '''
    speaker_dict = dict()
    text_dict = dict()
    economy_dict = dict()
    health_care_dict = dict()
    covid_dict = dict()
    supreme_court_dict = dict()
    for root, dirs, files in os.walk('archive'):
        for file in files:
            if file.endswith('.csv'):
                opening_csv_file(f'{root}/{file}', speaker_dict, text_dict, economy_dict, health_care_dict, covid_dict, supreme_court_dict)
    return speaker_dict, text_dict, economy_dict, health_care_dict, covid_dict, supreme_court_dict

def go():
    '''
    Runs the program
    :return: None
    '''
    connecting_to_database()

if __name__ == "__main__":
    go()
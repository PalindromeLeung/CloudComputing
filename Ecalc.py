import pandas as pd 
import numpy as np 
import os 
import re

# pwd = CloudComputing/Ecalc.py

# open stream data files and read each 
acceler_df = pd.read_csv("../Accelerdata/sample.csv",header = 0)

# task of calculating the momentum of the current object within a time slot
# feature to be created:
# time at second granularity
# distance of two continous coordination 
# addtional ideas1 : using FFT to create the signals patterns of the motion
# addtional ideas2 : using map reduce to run the task. 

# feature engineering on the first dimension -- Timestamp


def extracting_date(TimeStampString):
	index_T = TimeStampString.find("T")
	return TimeStampString[:index_T]

def extracting_hour(TimeStampString):
	index_T = TimeStampString.find("T")
	return TimeStampString[index_T+1:index_T+3]

def extracting_min(TimeStampString):
	index_T = TimeStampString.find("T")
	return TimeStampString[index_T+4:index_T+6]

def extracting_sec(TimeStampString):
	index_T = TimeStampString.find("T")		
	return TimeStampString[index_T+7,index_T+9]

def extracting_milisec(TimeStampString):
	# just keep the first four digits
	index_T = TimeStampString.find(".")		
	return TimeStampString[index_T+1:index_T+5]	

acceler_df['Date'] = acceler_df.Timestamp.apply(extracting_date)
acceler_df['Hour'] = acceler_df.Timestamp.apply(extracting_hour)
acceler_df['Minute'] = acceler_df.Timestamp.apply(extracting_min)
acceler_df['MiliSec'] = acceler_df.Timestamp.apply(extracting_milisec)

# calculating the distance between the two sampling period



# EDA on some of the fields to grasp the whole picture of the current dataset.









import pandas as pd 
import numpy as np 
import os 
import re
from scipy.fftpack import fft, ifft
from numpy.linalg import inv
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
	return TimeStampString[index_T+7:index_T+9]

def extracting_milisec(TimeStampString):
	# just keep the first four digits
	index_T = TimeStampString.find(".")		
	return TimeStampString[index_T+1:index_T+4]	

acceler_df['Date'] = acceler_df.Timestamp.apply(extracting_date)
acceler_df['Hour'] = acceler_df.Timestamp.apply(extracting_hour).astype(float)
acceler_df['Minute'] = acceler_df.Timestamp.apply(extracting_min).astype(float)
acceler_df['Second'] = acceler_df.Timestamp.apply(extracting_sec).astype(float)
acceler_df['MiliSec'] = acceler_df.Timestamp.apply(extracting_milisec).astype(float)

# treat those numbers as discreet signals and perform DFT on the matrix

matrix_x = acceler_df.as_matrix(columns = ['X','Y','Z','Hour','Minute','Second','MiliSec'])
fft_y = fft(matrix_x)

# take the transpose of matrix_x and the transpose of fft_y
x_trans = matrix_x.transpose()
y_trans = fft_y.transpose()

t1 = np.matmul(matrix_x, y_trans)
t2 = np.matmul(fft_y, x_trans)

# compute matrix multiplication



# EDA on some of the fields to grasp the whole picture of the current dataset.









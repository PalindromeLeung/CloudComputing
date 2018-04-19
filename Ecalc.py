import pandas as pd 
import numpy as np 
import os 
import warnings
warnings.filterwarnings("ignore")
import re
from scipy.fftpack import fft, ifft
from numpy.linalg import inv
from TimeFeatureEng import *
from SpaceFeatureEng import *
# pwd = CloudComputing/Ecalc.py

# open stream data files and read each 
# to be modiftied to read in chunks of data frame
path = '/Users/isprime/Documents/Garbages/CloudComputing/Accelerdata/'


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

def Angle(vect1, vect2):
    cosang = np.dot(v1, v2)
    sinang = la.norm(np.cross(v1, v2))
    return np.arctan2(sinang, cosang)

for i,filename in enumerate(os.listdir(path)):
	print(filename)
	print("Processing file " + str(i) + filename + "...")

	# chunks_df = pd.read_csv(filename,header = 0, chunksize=500)
	chunk_df = pd.read_csv("../Accelerdata/sample.csv", header = 0)
	# for chunk_df in chunks_df:
		# Time Features

		chunk_df['Date'] = chunk_df.Timestamp.apply(lambda x: extracting_date(x))
		chunk_df['Hour'] = chunk_df.Timestamp.apply(lambda x: extracting_hour(x)).astype(float)
		chunk_df['Minute'] = chunk_df.Timestamp.apply(lambda x: extracting_min(x)).astype(float)
		chunk_df['Second'] = chunk_df.Timestamp.apply(lambda x: extracting_sec(x)).astype(float)
		chunk_df['MiliSec'] = chunk_df.Timestamp.apply(lambda x: extracting_milisec(x)).astype(float)


		# Space Features

		# distance
		chunk_df['EcluDist'] = chunk_df.apply(lambda row: math.sqrt(row.X ** 2 + row.Y ** 2 + row.Z **2), axis = 1)

		# angles
		# chunk_df['Angle'] = chunk_df.apply(lambda row: Angle(zip(row.X,row.Y,row.Z), zip(row.shift(1).X,row.shift(1).Y,row.shift(1).Z)))

		# labels
		chunk_df['ID'] = i

		# treat those numbers as discreet signals and perform DFT on the matrix

		matrix_x = chunk_df.as_matrix(columns = ['X','Y','Z','Hour','Minute','Second','MiliSec'])
		fft_y = fft(matrix_x)

		# take the transpose of matrix_x and the transpose of fft_y
		x_trans = matrix_x.transpose()
		y_trans = fft_y.transpose()

		t1 = np.matmul(matrix_x, y_trans)
		t2 = np.matmul(fft_y, x_trans)











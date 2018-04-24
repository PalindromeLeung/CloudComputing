########################
# Author - ©isprime
# notes:
# change the directory to be compatible on both Rivanna and AWS
########################

# todos :
# 1. Add time evaluation methods
# 2. Add bandwith evaluation methods
# 3. Find a fancy map to represent the data

import pandas as pd 
import numpy as np 
import os 
import warnings
warnings.filterwarnings("ignore")
import re
from scipy.fftpack import fft, ifft
from numpy.linalg import inv
# from TimeFeatureEng import *
# from SpaceFeatureEng import *
import time
import sys, getopt
import matplotlib.pyplot as plt 
import seaborn as sns 

# pwd = CloudComputing/Ecalc.py

# open stream data files and read each 
# to be modiftied to read in chunks of data frame
path = '../Accelerdata/'
resultDirectory = '../ResultTimeEvlResult'


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

# def plot
# MACROS
CHUNKSIZE = 1000

# set the pallete for the plotting
sns.set(style="white", color_codes=True)

# create a dictionary for keeping the interval for Processing each of the file
fileInterval = dict()

for i,filename in enumerate(os.listdir(path)):
    print(filename)
    print("Processing file " + filename + "  |segment  " + str(i) + "|...")

    # chunks_df = pd.read_csv(filename,header = 0, chunksize=500)
        
    chunk_interval = 0
    for chunk_df in pd.read_csv("../Accelerdata/sample.csv", header = 0,chunksize = CHUNKSIZE,iterator = True):
        # create a dictionary for keeping the interval for Processing each of the chuck 
        # Time FeaturesBelow
        start = time.time()
        chunk_df['Date'] = chunk_df.Timestamp.apply(lambda x: extracting_date(x))
        chunk_df['Hour'] = chunk_df.Timestamp.apply(lambda x: extracting_hour(x)).astype(float)
        chunk_df['Minute'] = chunk_df.Timestamp.apply(lambda x: extracting_min(x)).astype(float)
        chunk_df['Second'] = chunk_df.Timestamp.apply(lambda x: extracting_sec(x)).astype(float)
        chunk_df['MiliSec'] = chunk_df.Timestamp.apply(lambda x: extracting_milisec(x)).astype(float)


        # Space Features

        # distance
        chunk_df['EcluDist'] = chunk_df.apply(lambda row: np.sqrt(row.X ** 2 + row.Y ** 2 + row.Z **2), axis = 1)

        # angles
        # chunk_df['Angle'] = chunk_df.apply(lambda row: Angle(zip(row.X,row.Y,row.Z), zip(row.shift(1).X,row.shift(1).Y,row.shift(1).Z)))

        # labels
        # chunk_df['ID'] = i

        # treat those numbers as discreet signals and perform DFT on the matrix

        matrix_x = chunk_df.as_matrix(columns = ['X','Y','Z','Hour','Minute','Second','MiliSec'])
        fft_y = fft(matrix_x)

        # take the transpose of matrix_x and the transpose of fft_y
        x_trans = matrix_x.transpose()
        y_trans = fft_y.transpose()

        t1 = np.matmul(matrix_x, y_trans)
        t2 = np.matmul(fft_y, x_trans)
        end = time.time()

        interval = end - start

        chunk_interval += interval

    # take the sum of the interval for each file 
    fileInterval[i] = chunk_interval

    fileInterval_df = pd.DataFrame.from_dict(fileInterval,orient = 'index',dtype = None)
    
    if not os.path.exists(resultDirectory):
        os.makedirs(resultDirectory,exist_ok = True)
            
    fileInterval_df.to_csv(resultDirectory + "/{}".format(filename))
    # another script for ploting 

fig = sns.jointplot("files", "time", data=fileInterval_df, kind="kde", space=0, color="g")
fig.savefig("output.png")


# Cloud Computing homework 3 - Map Reduce Framework 
# Sample code from the tutorials
# https://pythonhosted.org/mrjob/guides/quickstart.html

# pwd = /Users/isprime/Documents/Garbages/CloudComputing/CloudComputing


# 7. Attribute Information:
#    1. sepal length in cm
#    2. sepal width in cm
#    3. petal length in cm
#    4. petal width in cm
#    5. class: 
#       -- Iris Setosa
#       -- Iris Versicolour
#       -- Iris Virginica

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRIris1(MRJob):


    # 1) the minimum sepal length
    def mapper_get_sepal_len(self,key,line):
        Attributes = line.split(",")
        fields = len(Attributes)
        classification = Attributes[-1]
        sepal_length = float(Attributes[0])
        yield classification,sepal_length

    def reducer_min_sepal_len(self,categ,length):
        yield "Min Sepal Length", min(length)
    def steps(self):
        return[
        MRStep(mapper = self.mapper_get_sepal_len,
                reducer = self.reducer_min_sepal_len)]


class MRIris2(MRJob):

    # 2) the maximum petal width
    def mapper_get_petal_width(self,key,line):
        Attributes = line.split(",")
        categ = Attributes[-1]
        width = float(Attributes[3])
        yield categ,width

    def reducer_max_petal_width(self,categ, width):
        yield "Max Petal Width", max(width)
    def steps(self):
        return[
        MRStep(mapper = self.mapper_get_petal_width,
                reducer = self.reducer_max_petal_width)]


class MRIris3(MRJob):
    # 3) the average sepal width for the class “Iris Setosa”
    # def mapper_get_sepal_width_Setosa(self,key,line):
    def mapper_get_sepal_width_setosa(self,key,line):
        line_split = line.split(",")
        classification = line_split[-1]
        sep_width = line_split[1] # actually the 2nd field
        if classification =='setosa':
            yield classification,float(sep_width)
            yield key, float(sum(sep_width))/len(sep_width)



class MRIris4(MRJob):
    # 4) the difference in average sepal and petal length for all non-“Iris Setosa”

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_sepal_len,
                   reducer=self.reducer_min_sepal_len)]


if __name__ == '__main__':
    
    MRIris1.run()
    MRIris2.run()
    # MRIris3.run()
    # MRIris4.run()

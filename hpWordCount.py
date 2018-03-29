# Cloud Computing homework 3 - Map Reduce Framework 
# Process the Harry Potter and the Prisoner of Azkaban text
# https://pythonhosted.org/mrjob/guides/quickstart.html
# © isPrime 
# pwd = /Users/isprime/Documents/Garbages/CloudComputing/CloudComputing

from mrjob.job import MRJob 
from mrjob.step import MRStep
import re

class MRHarryPotter(MRJob):
	def mapper_get_words(self, key, line):
		letters_only = re.sub("[^a-zA-Z]"," ",line)
		letters_only = letters_only.lower()
		split2words = letters_only.split(" ")
		for i in range(len(split2words)):
			yield	split2words[i],1

	def combiner_sum_count(self,key, values):
		yield key,sum(values)

	def map_sort(self,word,count):
		count = '%04d' % int(count) 
		yield count, word
	def steps(self):
		return [
			MRStep(mapper = self.mapper_get_words,
				combiner = self.combiner_sum_count,
				reducer = self.map_sort)
		]


if __name__ == '__main__':
	MRHarryPotter.run()

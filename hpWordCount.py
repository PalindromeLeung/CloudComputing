# Cloud Computing homework 3 - Map Reduce Framework 
# Process the Harry Potter and the Prisoner of Azkaban text
# https://pythonhosted.org/mrjob/guides/quickstart.html
# © isPrime 
# pwd = /Users/isprime/Documents/Garbages/CloudComputing/CloudComputing

from mrjob.job import MRJob 
from mrjob.step import MRStep
import re


class MRMostUsedWord(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_sort_words)
        ]

    def mapper_get_words(self, _, line):
        # yield each word in the line
        line = re.sub("[^a-zA-Z]"," ",line)
        for word in line.split():
            yield (word.lower(), 1)

    def combiner_count_words(self, word, counts):
        # optimization: sum the words we've seen so far
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        # yield None, (sum(counts), word)
        yield None, ('%04d' % int(next(counts)), word)
    # discard the key; it is just None
    def reducer_sort_words(self, _, word_count_pairs):
        word_count_pairs = [(x,y) for x,y in word_count_pairs]
        result = sorted(word_count_pairs, key=lambda tup: tup[0],reverse = True)
        length = len(result)
        for i in range(length):
            yield result[i]

if __name__ == '__main__':
    MRMostUsedWord.run()

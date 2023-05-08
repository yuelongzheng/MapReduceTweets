from mrjob.job import MRJob
from mrjob.step import MRStep
import re


WORD_RE = re.compile(r"[a-zA-Z0-9']+")


class MRWordFrequency(MRJob):
    def mapper_get_words(self, _, line):
        # yield each word in the line
        for word in WORD_RE.findall(line):
            yield word.lower(), 1

    def combiner_count_words(self, word, counts):
        # sum the words we've seen so far
        yield word, sum(counts)

    def reducer_count_words(self, word, counts):
        yield word, sum(counts)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words)
        ]


if __name__ == '__main__':
    MRWordFrequency.run()

from mrjob.job import MRJob
from mrjob.step import MRStep
import time


class MRMostUsedWord(MRJob):

    SORT_VALUES = True

    def mapper_get_tags(self, _, line):
        yield line, 1

    def combiner_count_tags(self, tag, counts):
        # sum the words we've seen so far
        yield tag, sum(counts)

    def reducer_count_tags(self, tag, counts):
        yield None, (tag, sum(counts))

    def reducer_sort_tags(self, _, count_tags):
        for tag, count in sorted(count_tags):
            yield tag, count

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_tags,
                   combiner=self.combiner_count_tags,
                   reducer=self.reducer_count_tags),
            MRStep(reducer=self.reducer_sort_tags)
        ]


if __name__ == '__main__':
    start = time.time()
    MRMostUsedWord.run()
    end = time.time()
    time_elapsed = end - start
    print("Time elapsed: ", time_elapsed, "seconds")

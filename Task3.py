from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import pandas as pd

WORD_RE = re.compile(r"[a-zA-Z0-9']+")
df = pd.read_csv("au.csv")
city_list = df['city'].values.tolist()


class MRMostUsedWord(MRJob):
    def mapper_get_locations(self, _, line):
        found_gold_coast = re.search(r'\bGold Coast\b', line)
        if found_gold_coast:
            gold_coast = found_gold_coast.group(0)
            yield gold_coast, 1
        else:
            for location in WORD_RE.findall(line):
                if location.capitalize() in city_list:
                    yield location.capitalize(), 1

    def combiner_count_locations(self, location, counts):
        # sum the words we've seen so far
        yield location, sum(counts)

    def reducer_count_locations(self, location, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield location, sum(counts)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_locations,
                   combiner=self.combiner_count_locations,
                   reducer=self.reducer_count_locations)
        ]


if __name__ == '__main__':
    MRMostUsedWord.run()

from mrjob.job import MRJob
from mrjob.step import MRStep

class MapReduceCount(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:
            data = line.split(',')
            date = data[2].strip()
            if ' ' in date:
                year = date.split(' ')[0].split('/')[2]
                status_type = data[1].strip()
                if status_type == 'photo':
                    yield (year, 'photo'), 1
                elif status_type == 'video':
                    yield (year, 'video'), 1
                elif status_type == 'link':
                    yield (year, 'link'), 1
                elif status_type == 'status':
                    yield (year, 'status'), 1

    def reducer(self, key, values):
        yield key[0], (sum(values), key[1])

    def reducer_max(self, key, values):
        yield key, max(values)

    def steps(self):
        return [
            MRStep(mapper=self.mapper, \
                    reducer=self.reducer), \
            MRStep(reducer=self.reducer_max)]

if __name__ == '__main__':
    MapReduceCount.run()

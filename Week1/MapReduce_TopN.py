from mrjob.job import MRJob
from mrjob.step import MRStep

class MapReduceTopN(MRJob):
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
    
    def reducer_count(self, key, values):
        year, status_type = key
        yield year, (sum(values), status_type)
    
    def reducer_topN(self, key, values):
        N = 2
        sorted_values = sorted(values, reverse=True, key=lambda x: x[0])
        topN = sorted_values[:N]

        yield key, topN

    def steps(self):
        return [
             MRStep(mapper=self.mapper, reducer=self.reducer_count),
            MRStep(reducer=self.reducer_topN)]

if(__name__ == '__main__'):
    MapReduceTopN.run() 
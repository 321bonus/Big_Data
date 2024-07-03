from mrjob.job import MRJob
class MapReduceCount(MRJob):
    def mapper(self, _, line):

        if 'num_reactions' in line:
            return

        data = line.split(',')
        status_type = data[1].strip()
        num_reactions = data[3].strip()
        yield status_type, float(num_reactions)

    def reducer(self, key, values):
        lval = list(values)
        yield key, round(sum(lval)/len(lval),2)

if(__name__ == '__main__'):
    MapReduceCount.run()
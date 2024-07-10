from mrjob.job import MRJob
from mrjob.step import MRStep

class MapReduceleftJoin(MRJob):
    def mapper(self, _, line):
        data = line.strip().split(',')
        table = data[0]
        if table == 'FB2':
            yield data[1], ('FB2', data[2:])
        elif table == 'FB3':
            yield data[1], ('FB3', data[2:])
   
    def reducer(self, key, values):
        fb2_data = None
        fb3_data = None
       
        for value in values:
            if value[0] == 'FB2':
                fb2_data = value[1]
            elif value[0] == 'FB3':
                fb3_data = value[1]
       
        if fb2_data is not None:
            if fb3_data is not None:
                joined_data = fb2_data + fb3_data
            else:
                joined_data = fb2_data + [None] * 4  # Assuming 4 fields in FB3, adjust if necessary
        else:
            if fb3_data is not None:
                joined_data = [None] * 4 + fb3_data  # Assuming 4 fields in FB2, adjust if necessary
            else:
                return  # No matching records
       
        yield None, joined_data
   
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]

if __name__ == '__main__':
    MapReduceleftJoin.run()
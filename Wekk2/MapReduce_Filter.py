from mrjob.job import MRJob

class MapReduceFilter(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:  # Skip header
            data = line.split(',')
            datetime = data[2].strip()  # Get the datetime field
            
            if ' ' in datetime:  # Ensure datetime contains both date and time
                date_part = datetime.split(' ')[0]
                year = date_part.split('/')[2]
                date = date_part.split('/')[0]
                
                if year == '2018':
                    yield date, None

    def reducer(self, key, _):
        yield key, None

if __name__ == '__main__':
    MapReduceFilter.run()

from mrjob.job import MRJob

class MapReduceFilter(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:  # Skip header
            data = line.split(',')
            
            if len(data) >= 4:  # Ensure there are enough columns
                date = data[2].strip()  # Get the datetime field
                num_reactions = data[3].strip()  # Get the number of reactions
                
                if ' ' in date:
                    date_part = date.split(' ')[0]  # Extract the date part
                    date_components = date_part.split('/')
                    
                    if len(date_components) == 3:  # Ensure the date part has MM/DD/YYYY
                        month, day, year = date_components
                        
                        try:
                            if year == '2018' and int(num_reactions) > 2000:
                                status_type = data[0].strip()  # Get the status type
                                yield status_type, int(num_reactions)
                        except ValueError:
                            # Handle invalid integer conversion
                            pass

    def reducer(self, key, values):
        # Sum the number of reactions for each status type
        yield key, sum(values)

if __name__ == '__main__':
    MapReduceFilter.run()

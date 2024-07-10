from mrjob.job import MRJob

class MapReduceFilter(MRJob):
    def mapper(self, _, line):
        if 'status_id' not in line:  # Skip header
            data = line.split(',')
            
            if len(data) >= 4:  # Ensure there are enough columns
                datetime = data[2].strip()  # Get the datetime field
                
                if ' ' in datetime:  # Ensure datetime contains both date and time
                    date_part = datetime.split(' ')[0]
                    date_components = date_part.split('/')
                    
                    if len(date_components) == 3:  # Ensure the date part has MM/DD/YYYY
                        month, day, year = date_components
                        status_type = data[1].strip()  # Get the status type
                        
                        try:
                            if year == '2018':
                                if status_type == 'video':
                                    yield (year, 'video'), data
                                elif status_type == 'photo':
                                    yield (year, 'photo'), data
                                elif status_type == 'link':
                                    yield (year, 'link'), data
                                elif status_type == 'status':
                                    yield (year, 'status'), data
                        except ValueError:
                            # Handle invalid integer conversion
                            pass

if __name__ == '__main__':
    MapReduceFilter.run()

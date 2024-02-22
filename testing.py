from mrjob.job import MRJob
from mrjob.step import MRStep

# Define a class named RatingsBreakdown that inherits from MRJob
class RatingsBreakdown(MRJob):
    
    # Define the steps involved in the MapReduce job
    def steps(self):
        return [
            # Specify one step with a mapper and a reducer
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
        ]

    # Define the mapper function that processes input data
    def mapper_get_ratings(self, _, line):
        # Split the input line into fields based on the tab ('\t') delimiter
        (userID, movieID, rating, timestamp) = line.split('\t')
        # Output the rating as the key and the value 1
        yield rating, 1

    # Define the reducer function that aggregates and processes the mapped data
    def reducer_count_ratings(self, key, values):
        # Output the rating (key) and the sum of corresponding values (count of occurrences)
        yield key, sum(values)

# Main block: Ensure the MRJob runs when the script is executed directly
if __name__ == '__main__':
    RatingsBreakdown.run()

# Import necessary modules
from mrjob.job import MRJob
from mrjob.step import MRStep

# Define a class named ProductValueAnalyzer that inherits from MRJob
class ProductValueAnalyzer(MRJob):

    # Define the steps involved in the MapReduce job
    def steps(self):
        return [
            # Specify one step with a mapper and a reducer
            MRStep(mapper=self.mapper_extract_product_values,
                   reducer=self.reducer_calculate_total_value)
        ]

    # Define the mapper function that processes input data
    def mapper_extract_product_values(self, _, line):
        # Assuming the format is: ProductID\tQuantity\tPrice
        product_id, quantity, price = line.split('\t')
        # Output the product ID as the key and a tuple (quantity, price) as the value
        yield product_id, (float(quantity), float(price))

    # Define the reducer function that aggregates and processes the mapped data
    def reducer_calculate_total_value(self, key, values):
        # Calculate the total value for each product by summing quantity * price for all entries
        total_value = sum(quantity * price for quantity, price in values)
        # Output the product ID as the key and the total value as the value
        yield key, total_value

# Main block: Ensure the MRJob runs when the script is executed directly
if __name__ == '__main__':
    ProductValueAnalyzer.run()

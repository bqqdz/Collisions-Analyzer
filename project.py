import os
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

#A class to analyze motor vehicle collisions data
class CollisionsAnalyzer:
    """
    Attributes:
        df (pd.DataFrame): The DataFrame containing the collisions data.
    """
    def __init__(self, file_path):
        try:
            self.df = pd.read_csv(file_path)
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {file_path}")
        
    #Display the first few rows of the dataset
    def display_data(self):
        print(self.df.head())

    #Preprocess the data by converting the specified column to datetime
    def preprocess_data(self, date_column_name):
        try:
            self.df[date_column_name] = pd.to_datetime(self.df[date_column_name])
        except KeyError:
            raise KeyError(f"Column not found: {date_column_name}")
        
    #Analyze monthly trends in motor vehicle collisions
    def analyze_monthly_trends(self, date_column_name):
        try:
            monthly_collisions = self.df.resample('M', on=date_column_name).size()
            return monthly_collisions
        except KeyError:
            raise KeyError(f"Column not found: {date_column_name}")
        
    #Plot monthly trends in a bar chart
    def plot_monthly_trends(self, monthly_collisions):
        plt.figure(figsize=(10, 6))
        monthly_collisions.plot(kind='bar', color='skyblue')
        plt.title('Yearly Trends in Motor Vehicle Collisions')
        plt.xlabel('T')
        plt.ylabel('Total Collisions')
        plt.show()

    #first data processing method
    def process_method1(self):
        try:
            result_method1 = self.df.groupby('BOROUGH')['NUMBER OF PERSONS INJURED'].mean()
            return result_method1
        except KeyError:
            raise KeyError("Column not found: 'BOROUGH'")

    #second data processing method
    def process_method2(self):
        try:
            result_method2 = self.df['CONTRIBUTING FACTOR VEHICLE 1'].value_counts().head(10)
            return result_method2
        except KeyError:
            raise KeyError("Column not found: 'CONTRIBUTING FACTOR VEHICLE 1'")
        

#A class to manage the storage of collisions date
#Parameters: date_column_name (str): The name of the date column
class DataStorageManager:
    """
    Attributes:
        data (pd.DataFrame): The DataFrame containing the collisions data.
    """
    def __init__(self, data):
        self.data = data

    #Save data to MongoDB
    def save_to_mongodb(self):
        try:
            client = MongoClient('mongodb://localhost:27017/')
            db = client['motor_vehicle_collisions_db']
            collection = db['collisions_data']
            collection.insert_many(self.data.to_dict('records'))
        except Exception as e:
            raise RuntimeError(f"Error saving data to MongoDB: {str(e)}")
        finally:
            client.close()


if __name__ == "__main__":
    try:
        # Get the path to the script's directory
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Combine the script's directory with the CSV file name
        file_path = os.path.join(script_directory, 'Motor_Vehicle_Collisions.csv')

        # Create an instance of CollisionsAnalyzer
        analyzer = CollisionsAnalyzer(file_path)

        # Display the first few rows of the dataset
        analyzer.display_data()

        # Specify the date column name
        date_column_name = 'CRASH DATE'

        # Preprocess the data
        analyzer.preprocess_data(date_column_name)

        # Analyze and plot monthly trends
        monthly_collisions = analyzer.analyze_monthly_trends(date_column_name)
        analyzer.plot_monthly_trends(monthly_collisions)

        # Process data using method 1
        result_method1 = analyzer.process_method1()

        # Display the result of method 1 in a chart
        plt.figure(figsize=(10, 6))
        result_method1.plot(kind='bar', color='lightgreen')
        plt.title('Average number of people injured per town')
        plt.xlabel('Area')
        plt.ylabel('Average Number of People Injured')
        plt.show()

        # Process data using method 2
        result_method2 = analyzer.process_method2()

        # Display the result of method 2 in a chart
        plt.figure(figsize=(10, 6))
        result_method2.plot(kind='bar', color='coral')
        plt.title('Count of collisions per contributing factor')
        plt.xlabel('Cause of Collision')
        plt.ylabel('Number of Collisions')
        plt.show()

        # Create an instance of DataStorageManager
        storage_manager = DataStorageManager(analyzer.df)

        # Save data to MongoDB
        storage_manager.save_to_mongodb()

    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")


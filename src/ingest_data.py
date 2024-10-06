import os
import zipfile
from abc import ABC, abstractmethod

import pandas as pd

# defining a abstract class for my data ingester fucntion
# this will help  me to create a class that will be able to ingest data from different sources, Also it will give security.
# also it wll
# I use an abstract class while designing large functional units or when I want to provide a common interface for different implementations of a component.


# ABC: This is the base class for defining Abstract Base Classes.
class DataIngestor(ABC):  # inherit from ABC
    @abstractmethod  # @abstractmethod: This decorator indicates that the abstract method ingest must be implemented by any subclass of DataIngestor.
    def ingest(
        self, file_path: str
    ) -> pd.DataFrame:  # def ingest(self, file_path: str) -> pd.DataFrame: This method signature specifies that it takes a string parameter (file_path) and returns a pandas DataFrame.
        "this is my abstract method to ingest data from a given file"
        pass


# implenting a concrete class for zip ingestion (get zip file and return a dataframe)
class ZipDataIngestor(DataIngestor):
    def ingest(self, file_path: str) -> pd.DataFrame:
        """Extracts a .zip file and returns the content as a pandas DataFrame."""
        # Ensuring the file is a .zip
        if not file_path.endswith(".zip"):
            raise ValueError("The provided file is not a .zip file.")

        # Extract the zip file and place it in extracted_data
        with zipfile.ZipFile(file_path, "r") as zip_file:
            zip_file.extractall(
                "extracted_data"
            )  # It extracts all contents of the ZIP file into a directory named extracted_data. If this directory does not exist, it will be created automatically.

        # find the extracted csv file (assuming  it is the only one csv file inside zip)
        extracted_files = os.listdir("extracted_data")
        csv_files = [f for f in extracted_files if f.endswith(".csv")]

        # checking if there are any csv files or not in csv_files
        if len(csv_files) == 0:
            raise FileNotFoundError("No CSV file found in the extracted data.")
        if len(csv_files) > 1:
            raise ValueError(
                "More than one CSV file found in the extracted data. Please ensure there is only one or specify"
            )

        # reading the csv into a DataFrame
        # Assuming there is exactly one CSV file, this code constructs its full path and reads it into a pandas DataFrame using pd.read_csv.
        csv_file_path = os.path.join("extracted_data", csv_files[0])
        df = pd.read_csv(csv_file_path)
        # The primary purpose of os.path.join() is to create a valid file path by joining multiple directory or file names together using the appropriate separator for the operating system (e.g., / for Unix-like systems and \ for Windows).

        # returning the extracted data frame
        return df


#  A factory class is a design pattern used to create objects without specifying the exact class of object that will be created.
class DataIngestFactory:
    @staticmethod  # @staticmethod: This decorator indicates that the method get_data_ingestor does not require access to any instance-specific data (i.e., it does not need to access self). It can be called on the class itself rather than on an instance of the class.
    def get_data_ingestor(file_extension: str) -> DataIngestor:
        """Returns a DataIngestor instance based on the file extension."""
        if file_extension == ".zip":
            return ZipDataIngestor()
        else:
            raise ValueError(
                f"Unsupported file extension. Only .zip files are supported at this time not '{file_extension}'."
            )


if __name__ == "__main__":
    # specify the file path
    # Prefixing the string with r tells Python to treat backslashes as literal characters and not as escape characters.
    file_path = r"C:\Users\Admin\Desktop\Contains_proj\House_Price_Mlops\data\archive.zip"

    # determining the file extension
    file_extension = os.path.splitext(file_path)[1]

    # get the proper appropriate DataIngestor
    data_ingestor = DataIngestFactory.get_data_ingestor(file_extension)

    # Ingest the data and load it into a dataframe
    df = data_ingestor.ingest(file_path)

    # now df contains the DataFrame from the extracted csv
    print(df.head())


# ---------------------------ignore-------------------------
# ----my learning------
# zip_ingestor = ZipDataIngestor()
# data_frame = zip_ingestor.ingest('data.zip')
# print(data_frame.head())  # Display the first 5 rows of the DataFrame
# This will create an instance of ZipDataIngestor, call its ingest method with a specified ZIP file path, and print out the first few rows of the resulting DataFrame.

# Using the factory design pattern alongside abstract classes provides several advantages in software development, particularly in terms of code organization, maintainability, and flexibility. Here’s a detailed explanation of why these approaches are beneficial:
# Benefits of Using Abstract Classes:-
# Code Reusability:
# Abstract classes allow you to define common behaviors and attributes that can be shared across multiple subclasses. This reduces code duplication and promotes reusability, as common functionality can be implemented once in the abstract class and inherited by subclasses 25.
# Enforced Consistency:
# By defining abstract methods in an abstract class, you ensure that all subclasses implement these methods. This enforces a consistent interface across different implementations, which is particularly useful in large codebases where multiple developers are involved 24.
# Encapsulation:
# Abstract classes can encapsulate common state and behavior while hiding implementation details from the user. This abstraction simplifies the interface presented to users and allows changes to the internal workings without affecting dependent code 14.
# Flexibility and Extensibility:
# Abstract classes provide a framework that allows developers to extend functionality easily. If new types of data ingestors need to be added (e.g., for JSON or XML), new subclasses can be created without altering existing code 23.
# Polymorphism:
# Abstract classes enable polymorphic behavior, allowing objects of different subclasses to be treated as objects of the abstract class type. This is useful for writing generic code that can operate on a variety of data types 35.
# Benefits of Using the Factory Design Pattern
# Decoupling Object Creation:
# The factory pattern separates the process of object creation from its usage. This means that the client code does not need to know about the specific classes being instantiated, which reduces dependencies and improves maintainability 13.
# Simplified Object Management:
# By using a factory, you can manage the instantiation logic in one place. This centralization makes it easier to modify or extend how objects are created without affecting other parts of the application 23.
# Support for Multiple Implementations:
# The factory pattern allows for easy integration of multiple implementations of an interface (or abstract class). For instance, if you introduce new file formats in your data ingestion system, you can simply add new ingestor classes and update the factory method accordingly without changing existing client code 45.
# Improved Code Organization:
# The combination of factories and abstract classes leads to better-organized code structures, making it easier for developers to navigate and understand the relationships between different components 12.
# Error Handling:
# Factories can encapsulate error handling related to object creation (e.g., handling unsupported file types). This keeps client code clean and focused on its primary responsibilities rather than managing instantiation logic 34.
# Conclusion
# In summary, using abstract classes along with the factory design pattern enhances your software architecture by promoting reusability, consistency, and flexibility while simplifying object management and improving maintainability. These design principles are particularly valuable in large-scale applications where multiple developers collaborate, as they provide a clear structure that facilitates understanding and modification over time. By adopting these patterns, you create a more robust and adaptable codebase that can evolve with changing requirements.

YouTube Data Harvesting and Warehousing
This project aims to collect data from YouTube channels, store it in a PostgreSQL database, and perform various analyses on the collected data. The project utilizes Python for data collection, manipulation, and analysis, and SQL for database management.

TOOLS AND LIBRARIES USED: this project requires the following components:

PYTHON: Python is a powerful programming language renowned for being easy to learn and understand. Python is the primary language employed in this project for the development of the complete application, including data retrieval, processing, analysis, and visualisation.

GOOGLE API CLIENT: The googleapiclient library in Python facilitates the communication with different Google APIs. Its primary purpose in this project is to interact with YouTube's Data API v3, allowing the retrieval of essential information like channel details, video specifics, and comments. By utilizing googleapiclient, developers can easily access and manipulate YouTube's extensive data resources through code.

POSTGRESQL: PostgreSQL is an advanced, enterprise-class open-source relational database that supports both SQL (relational) and JSON (non-relational) querying. It is a highly stable database management system backed by more than 20 years of community development.

STREAMLIT: Streamlit library was used to create a user-friendly UI that enables users to interact with the programme and carry out data retrieval and analysis operations.

REQUIREMENTS
1.Python 
2.Google API Client Library (google-api-python-client)
3.SQLAlchemy
4.Psycopg2
5.Pandas
6.Streamlit

USAGE
1.Enter the YouTube channel ID in the provided input field.
2.Click on "Fetch Data" to collect channel information, video details, and comments from the specified channel.
3.Click on "Push to Database" to store the collected data in the PostgreSQL database.
4Select a question from the dropdown menu and click on "Execute" to perform various analyses on the collected data.

FEATURES: The following functions are available in the YouTube Data Harvesting and Warehousing application: Retrieval of channel and video data from YouTube using the YouTube API.

Search and retrieval of data from the SQL database using different search options.

Contributions:
Contributions to this project are welcome! If you have suggestions, feature requests, or improvements, please feel free to submit a pull request.

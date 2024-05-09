# YOUTUBE-DATA-HARVESTING-AND-WAREHOUSING-

This project aims to  create a Streamlit application that allows users to access and analyze data from multiple YouTube channels

## Skills take away From This Project :

 __Python scripting__

 __Data Collection__

 __Streamlit__

 __API integration__

 __MongoDB database__

 __Data Management using SQL__  




### Approach:

1. Launch a Streamlit app: Streamlit is an excellent platform for instantly and simply developing data visualisation and analysis applications. I have utilised Streamlit to design a basic user interface that allows users to enter a YouTube channel ID, examine channel details, and choose which channels to migrate to the data warehouse.

2. Access to the YouTube API: will need to use the YouTube API to get channel and video data. It's possible to use the Python Google API client library to make requests to the API.

3. Store and clean data: After retrieving data from the YouTube API, save it in a suitable format for temporary storage before transferring it to a data warehouse. We can use Pandas DataFrames or other in-memory data structures.In this project, I used MongoDB for it.

4. Data migration to a SQL data warehouse: After collecting data from various channels, transfer it to a SQL data warehouse. This can be accomplished using a SQL database such as MySQL or PostgreSQL.

5. Query the SQL data warehouse: SQL queries can be used to connect tables and pull data from certain channels based on user input.

6.  Display data in the Streamlit app: Finally, display the information gathered in the Streamlit app. 


### Tools:

1. Streamlit : Streamlit is a Python-based library that allows data scientists to easily create free machine learning applications. Streamlit allows you to display descriptive text and model outputs, visualize data and model performance and modify model inputs through the UI using sidebars.

2. Google API Client : Python's googleapiclient module supports connection with various Google APIs. Its primary goal in this project is to connect with YouTube's Data API v3, which allows for the retrieval of important information such as channel data, video specifications, and comments.

3. MongoDB : MongoDB is a document database with the scalability and flexibility that you want with the querying and indexing that you need.MongoDB stores data in flexible, JSON-like documents, meaning fields can vary from document to document and data structure can be changed over time

4. PostgreSQL : PostgreSQL is an advanced, enterprise-class, and open-source relational database system. PostgreSQL supports both SQL (relational) and JSON (non-relational) querying. It is a highly stable database backed by more than 20 years of development by the open-source community.





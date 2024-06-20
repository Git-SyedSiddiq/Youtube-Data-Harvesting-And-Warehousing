# Youtube-Data-Harvesting-And-Warehousing
This is a project to retrieve the channel data from the youtube website using the api key 
First,get the Api key from the Google Developer Console.
Using that api key develop a connection to Youtube using googleapiclient.discovery library.
Next get the channel data,video ids,video details,comment details and playlist ids using the above connection by referring to the Youtube Data site (https://developers.google.com/youtube/v3).
Insert all the collected data into MongoDB using Pymongo library
From MongoDb migrate it to MySql using mysql.connector library
Using all the data from MongoDB and MySql we can easily access the data through Streamlit application which aloows to interact with your data and provide feedback quickly.

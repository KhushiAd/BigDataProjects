# Analyzing Millions of NYC Fire Incident Dispatch Data

For this project, we will be loading the FDNY’s data to Opensearch using Socrata and visualizing the findings. 

1. The docker image needs to be built on the instance with Dockerfile, requirements.txt and main.py to the EC2 instance.  

2. Then we use a series of commands to build the docker and run it.  

    a. To Build:
     ```
   docker build -t project01:1.0 .
     ``` 

    b. To Run:
   ```
    docker run \ 
    
    -e INDEX_NAME="dispatch" \ 
    
    -e DATASET_ID="8m42-w767" \ 
    
    -e APP_TOKEN=[YOUR_SOCRATA_API_TOKEN_HERE] \ 
    
    -e ES_HOST=[YOUR_ELASTIC_SEARCH_HOST_HERE] \ 
    
    -e ES_USERNAME=[YOUR_USERNAME_HERE] \ 
    
    -e ES_PASSWORD=[YOUR_PASSWORD_HERE] \ 
    
    project01:1.0 --page_size=[ROWS_PER_PAGE] --num_pages=[NUMBER_OF_PAGES]
   ```

3. From there, the data loads to Opensearch and will take some time based on the number of pages chosen. We open the Opensearch and move on to Visualization. For this project, we have created 5 visualizations that are inside a folder named asset in the zip file! 

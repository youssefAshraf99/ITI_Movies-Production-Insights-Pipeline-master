**Abstract**

The Movies Production Insights Pipeline Project aimed to design and implement an efficient data pipeline for the movie industry. The primary objectives included:

- Data Integration: Consolidate diverse data sources related to persons, movies, genres, production, and more into a centralized data warehouse.

- Automation: Utilize Apache Airflow for automated orchestration of data pipelines, ensuring timely and accurate data processing.

- Transformation: Employ Pyspark for effective data extraction and transformation, enabling the conversion of raw data into a structured format suitable for analysis.

- Staging Area: Implement MongoDB as a NoSQL staging area for interim data storage, providing flexibility and scalability.

- ETL Process: Utilize Talend for Extract, Transform, Load (ETL) processes, facilitating the seamless transfer of data from the staging area to the PostgreSQL data warehouse.

- Change Data Capture (CDC): Implement a CDC strategy using the Meta_Data table to capture and track changes and use logic to archive all old files and receive only the latest files, ensuring the data warehouse reflects the latest information.

- Visualization: Integrate Power BI for insightful visualization, allowing users to explore trends, patterns, and relationships within the movie industry.
This documentation provides a high-level overview of the project's objectives, methodologies, and implementation strategies.

**Meet OurTeam**

* Omar Mahmoud
* Osama Tarek
* Abdullah Mahmoud
* Ataa Mohamed
* Mohamed Selem
* Youssef Ashraf
  
    ![team](https://github.com/3amory99/Movies-Production-Insights-Pipeline/blob/master/03_Screenshots/Team.png)

**Project Overview**

The Movies Production Insights Pipeline – a revolutionary project poised to transform the way we navigate the dynamic realm of filmmaking. Leveraging data sourced from the Movie Production Database

* Descriptive Analysis on Movies Dataset using BI-Dashboard.
* Our mission: to empower and provide your business team with a streamlined solution and data pipeline.
* Our pipeline: encapsulates production details, movie insights, personnel data, crew information, and critical performance measures. This succinct integration allows for swift and informed decision-making in an industry where timing and precision are paramount.

**Data Sources**

* JSON Files
  * [JSON Files Dataset](https://github.com/3amory99/Movies-Production-Insights-Pipeline/tree/master/Reference_data/Archive)
  * Country (88)
  * Department (12)
* XML Files
  * [XML Files Dataset](https://github.com/3amory99/Movies-Production-Insights-Pipeline/tree/master/Reference_data/Archive)
  * Gender (3)
  * Genere (20)
* CSV Files
  * [CSV Files Dataset](https://github.com/3amory99/Movies-Production-Insights-Pipeline/tree/master/Reference_data/Archive)
  * Language (88)
  * Language Role (2)
* Relational Database
  * Postgres
  * movie (4803)
  * movie_genre (12160)
  * movie_cast (106257)
  * movie_company (13677)
  * movie_crew (129581)
  * movie_languages (11740)
  * person (104842)
  * production_company (5047)
  * production_country (250)
  * ![Relationa_Database_Schema](https://github.com/3amory99/Movies-Production-Insights-Pipeline/blob/master/03_Screenshots/01.png)

**Movies Production Pipeline Architecture Model**

![Architecture Model](https://github.com/3amory99/Movies-Production-Insights-Pipeline/blob/master/03_Screenshots/The%20Architecture%20Model.jpg)

* Diverse Data Sources:
Ingesting data from JSON, XML, CSV, and traditional Relational Databases.

* Spark-Powered Ingestion:
Harnessing the speed and scalability of Apache Spark for efficient data ingestion.

* Staging in MongoDB:
Leveraging MongoDB as our staging area for seamless data transformation.

* ETL Magic with Talend OS:
Unveiling the ETL prowess of Talend Open Studio for crafting a robust Data Warehouse.

* Cubing for Dimensional Brilliance:
Building insightful cubes to unlock the dimensions of our movie data.

* Power BI Dashboards:
Transforming data into actionable insights through Power BI dashboards


**Data Ingestion & Staging Area**

* **Data Ingestion**

    * The data ingestion and staging area form crucial components of a data pipeline, where PySpark plays key roles in efficiently transforming raw data into a format ready for analysis and insights.
    * Files
      - Step 1: Read Data
        The function uses Apache Spark to read data from the specified path, creating a Spark DataFrame.
      - Step 2: Convert to Pandas DataFrame
        The Spark DataFrame is then converted to a Pandas DataFrame. This facilitates compatibility with other data processing tools and libraries.
      - Step 3: Extract Filename and Archive
        The function leverages the input_file_name() function to extract the filename of the processed file. It then uses the Archiving function to archive the file, ensuring that it is not processed again in subsequent DAG runs, preventing duplication.
      - Step 4: Return Result
        The function returns the Pandas DataFrame containing the data, making it available for further processing within the data pipeline.
        These steps outline the general process followed by each function in extracting data from different file sources and ensuring data integrity through archiving.
 
    * Postgres Database
      - Connect to Postgres relational database and read the relevant data using Pyspark connections.
      
* **Staging Area (MongoDB)**
    * The Function is responsible for loading data into the MongoDB Staging Area for the Movies Production Insights Pipeline Project. This function takes various DataFrames as input and writes them to their respective collections in the MongoDB Staging Area.
    * **Purpose**: Transforms and loads data into the MongoDB Staging Area collections.
      
    * Before Loading: 
    ![satging](https://github.com/3amory99/Movies-Production-Insights-Pipeline/blob/master/03_Screenshots/03.png)

    * After Loading:
    ![satging_2](https://github.com/3amory99/Movies-Production-Insights-Pipeline/blob/master/03_Screenshots/06.png)

* **CDC Implementation**

   * Files (Archiving)
        - Before running MoviesStagingArea_ETL Dag

  ![satging_file_1](https://github.com/3amory99/Movies-Production-Insights-Pipeline/blob/master/03_Screenshots/File%20CDC%201.jpeg)

        - After running MoviesStagingArea_ETL Dag
  
  ![satging_file_2](https://github.com/3amory99/Movies-Production-Insights-Pipeline/blob/master/03_Screenshots/File%20CDC%202.jpeg)


    * Database (Metadata)
      The following functions are designed for data extraction from a PostgreSQL database utilizing Change Data Capture (CDC) principles. Each function performs a selective extraction based on the last update           date and subsequently updates the metadata information in the meta_data table.
    1. UpdateMetaData
       Purpose: Updates metadata information in the meta_data table, indicating the last update date for a specific table.

       - Before Update Meta Table :

         -![database _cdc](https://github.com/3amory99/Movies-Production-Insights-Pipeline/blob/master/03_Screenshots/02.png)

       - After Update Meta Table :

         -![database _cdc](https://github.com/3amory99/Movies-Production-Insights-Pipeline/blob/master/03_Screenshots/05.png)


**Airflow Dags**
   * MoviesStagingArea_ETL Dag

     -![first dag](https://github.com/3amory99/Movies-Production-Insights-Pipeline/blob/master/03_Screenshots/04.png)
       
   * Talend_ETL Dag
     
     -![second dag](https://github.com/3amory99/Movies-Production-Insights-Pipeline/blob/master/03_Screenshots/07.png)

   * Trigger Talend ETL Task
      - The Trigger_Talend_ETL task serves as a pivotal component in the orchestration of the Movies Production Insights Pipeline.
      - It is responsible for initiating the execution of the second DAG, Talend_ETL, once the first DAG successfully completes its tasks.
      - Once triggered, the task initiates the execution of the Talend_ETL DAG, signaling the beginning of the Extract, Transform, and Load (ETL) operations for building DWH.
    
**Movies Production Insights Pipeline Project Data Warehouse**

  ![database _cdc](https://github.com/3amory99/Movies-Production-Insights-Pipeline/blob/master/03_Screenshots/14.png)

  * The data warehouse for the Movies Production Insights Pipeline Project is structured using a denormalized star schema, optimizing the schema for efficient querying and analysis of movie-related data. The schema includes four dimension tables and one fact table, each playing a crucial role in providing comprehensive insights into movie production data.
    
  * Movies Production DWH:    
      - Movie Dimension (MovieDim)
      - Person Dimension (PersonDim)
      - Production Dimension (ProductionDim)
      - Date Dimension (DimDate)
      - Movies Fact (MoviesFact)

  * Our Fact Measures:
      - Popularity (popularity):
        o Popularity score of the movie.
      - Revenue (revenue):
        o Revenue generated by the movie.
      - Vote Count (vote_count):
        o Count of votes received by the movie.
      - Vote Average (vote_average):
        o Average vote rating is given to the movie.

**Talend Jobs**

These metadata categories allow you to define the structure of your data at design time, making your Talend jobs more modular and easier to maintain. By using metadata, you can also achieve better reusability of your job designs and streamline the management of connections and data structures.

  ![talend metadata](https://github.com/3amory99/Movies-Production-Insights-Pipeline/blob/master/03_Screenshots/08.png)

  * Movie Dimension (MovieDim)

  ![Movie Dimension](https://github.com/3amory99/Movies-Production-Insights-Pipeline/blob/master/03_Screenshots/09.png)
    
  * Production Dimension (ProductionDim)

  ![Production Dimension](https://github.com/3amory99/Movies-Production-Insights-Pipeline/blob/master/03_Screenshots/11.png)
    
  * Person Dimension (PersonDim)

  ![Person Dimension](https://github.com/3amory99/Movies-Production-Insights-Pipeline/blob/master/03_Screenshots/10.png)
    
  * Date Dimension (DimDate)

  ![Date Dimension](https://github.com/3amory99/Movies-Production-Insights-Pipeline/blob/master/03_Screenshots/12.png)
    
  * Movies Fact (MoviesFact)

  ![Movies Fact](https://github.com/3amory99/Movies-Production-Insights-Pipeline/blob/master/03_Screenshots/13.png)



**Reporting & Insights**
  * Insights derived from the data warehouse were visualized using Power BI.
    
  * The Movies Insights Dashboard presented key metrics, including the count of movies and revenue by company, production revenue by language type, count of movies and revenue by person, and individual movie details with revenue

  * **Power BI**
    
     ![dashboard](https://github.com/3amory99/Movies-Production-Insights-Pipeline/blob/master/03_Screenshots/16.jpeg)


1. Clone the repository:

   ```bash
   git clone https://github.com/3amory99/Movies-Production-Insights-Pipeline.git

    
# ITI-_Graduation_Project_Movies-Production-Insights-Pipeline-master

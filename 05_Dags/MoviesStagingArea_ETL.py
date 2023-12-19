# import Libraries
import os
import pendulum
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from airflow.decorators import dag,task
from airflow.operators.dagrun_operator import TriggerDagRunOperator


# Spark session configuration
spark = SparkSession.builder.appName("MoviesStagingArea_ETL")\
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")\
    .getOrCreate()

# Define Variables
postgres_properties = {
        "url": "jdbc:postgresql://localhost:5432/MoviesDB",
        "driver": "org.postgresql.Driver",
        "user": "postgres",
        "password": "2021"
        }

# Define Functions
def Archiving(file):
    os.system(f'mv {file} /home/el-neshwy/ITI_GP/Reference_data/Archive')

def UpdateMetaData(tableName,lastDate):
    MetaData = spark.read.jdbc(url=postgres_properties["url"],table=f"(select * from movies.meta_data) t",properties=postgres_properties)
    MetaDataDF = pd.DataFrame(MetaData.collect(),columns=["tablename","lastdate"])
    condition = MetaDataDF["tablename"] == tableName
    MetaDataDF.loc[condition, "lastdate"] = lastDate
    MetaDataSpark = spark.createDataFrame(MetaDataDF)    
    MetaDataSpark.write.format("jdbc").options(url=postgres_properties["url"],driver = postgres_properties["driver"],dbtable = "movies.meta_data",user = postgres_properties["user"],password = postgres_properties["password"]).mode("overwrite").save()

# Dag Configuration
@dag(
    dag_id='MoviesStagingArea_ETL',
    schedule='@daily',
    start_date=pendulum.datetime(2023,10,9),
    catchup = False
)

def MoviesStagingArea_ETL():

    @task
    def CountryJson():
        countryJson =  spark.read.json('/home/el-neshwy/ITI_GP/Reference_data/01_JsonSource/Country')
        countryDF = pd.DataFrame(countryJson.collect(),columns=countryJson.columns)
        FileName = countryJson.select(input_file_name()).distinct().collect()[0][0]
        Archiving(FileName[7:])
        return countryDF

    @task
    def DepartmentJson():
        departmentJson =  spark.read.json('/home/el-neshwy/ITI_GP/Reference_data/01_JsonSource/Department')
        departmentDF = pd.DataFrame(departmentJson.collect(),columns=departmentJson.columns)
        FileName = departmentJson.select(input_file_name()).distinct().collect()[0][0]
        Archiving(FileName[7:])
        return departmentDF

    @task
    def GenderXml():
        Schema = StructType([
        StructField("gender_id", IntegerType(), True),
        StructField("gender", StringType(), True)
        ])
        genderXml =  spark.read.format('xml').schema(Schema).options(rowTag='root') .option('rowTag','row').load('/home/el-neshwy/ITI_GP/Reference_data/02_XmlSource/Gender')
        genderDF = pd.DataFrame(genderXml.collect(),columns=genderXml.columns)
        FileName = genderXml.select(input_file_name()).distinct().collect()[0][0]
        Archiving(FileName[5:])
        return genderDF
    
    @task
    def GenreXml():
        Schema = StructType([
        StructField("genre_id", IntegerType(), True),
        StructField("genre_name", StringType(), True)
        ])
        genreXml =  spark.read.format('xml').schema(Schema).options(rowTag='root') .option('rowTag','row').load('/home/el-neshwy/ITI_GP/Reference_data/02_XmlSource/Genre')
        genreDF = pd.DataFrame(genreXml.collect(),columns=genreXml.columns)
        FileName = genreXml.select(input_file_name()).distinct().collect()[0][0]
        Archiving(FileName[5:])
        return genreDF

    @task
    def LangaugeCsv():
        languageCsv =  spark.read.csv('/home/el-neshwy/ITI_GP/Reference_data/03_CsvSource/Langauge',header=True)
        languageDF = pd.DataFrame(languageCsv.collect(),columns=languageCsv.columns)
        FileName = languageCsv.select(input_file_name()).distinct().collect()[0][0]
        Archiving(FileName[7:])
        return languageDF
    
    @task
    def LanguageRoleCsv():
        languageroleCsv =  spark.read.csv('/home/el-neshwy/ITI_GP/Reference_data/03_CsvSource/LanguageRole',header=True)
        languageroleDF = pd.DataFrame(languageroleCsv.collect(),columns=languageroleCsv.columns)
        FileName = languageroleCsv.select(input_file_name()).distinct().collect()[0][0]
        Archiving(FileName[7:])
        return languageroleDF

    @task
    def MoviePostgres():
        movie = spark.read.jdbc(url=postgres_properties["url"],table="(select * from movies.movie where indate > (select lastdate from movies.meta_data where tablename = 'movie')) t",properties=postgres_properties)
        movieDF = pd.DataFrame(movie.collect(),columns=["movie_id", "title", "budget", "homepage", "overview", "popularity", "release_date", "revenue", "runtime", "movie_status", "tagline", "vote_average", "vote_count","indate"])
        if (len(movieDF) > 0):
            LastDate = spark.read.jdbc(url=postgres_properties["url"],table="(select max(indate) as lastdate from movies.movie) t",properties=postgres_properties)
            LastDataDF = pd.DataFrame(LastDate.collect(),columns=["lastdate"])
            UpdateMetaData('movie',LastDataDF["lastdate"][0])
            return movieDF
        
    @task
    def MovieGenrePostgres():
        movieGenre = spark.read.jdbc(url=postgres_properties["url"],table="(select * from movies.movie_genres where indate > (select lastdate from movies.meta_data where tablename = 'movieGenre')) t",properties=postgres_properties)
        movieGenreDF = pd.DataFrame(movieGenre.collect(),columns=["movie_id","genre_id","indate"])
        if (len(movieGenreDF) > 0):
            LastDate = spark.read.jdbc(url=postgres_properties["url"],table="(select max(indate) as lastdate from movies.movie_genres) t",properties=postgres_properties)
            LastDataDF = pd.DataFrame(LastDate.collect(),columns=["lastdate"])
            UpdateMetaData('movieGenre',LastDataDF["lastdate"][0])
            return movieGenreDF
    
    @task
    def MovieCastPostgres():
        movieCast = spark.read.jdbc(url=postgres_properties["url"],table="(select * from movies.movie_cast where indate > (select lastdate from movies.meta_data where tablename = 'movieCast')) t",properties=postgres_properties)
        movieCastDF = pd.DataFrame(movieCast.collect(),columns=["movie_id", "person_id", "character_name", "gender_id", "cast_order","indate"])
        if (len(movieCastDF) > 0):
            LastDate = spark.read.jdbc(url=postgres_properties["url"],table="(select max(indate) as lastdate from movies.movie_cast) t",properties=postgres_properties)
            LastDataDF = pd.DataFrame(LastDate.collect(),columns=["lastdate"])
            UpdateMetaData("movieCast",LastDataDF["lastdate"][0])
            return movieCastDF
        
    @task
    def MovieCompanyPostgres():
        MovieCompany = spark.read.jdbc(url=postgres_properties["url"],table="(select * from movies.movie_company where indate > (select lastdate from movies.meta_data where tablename = 'movieCompany')) t",properties=postgres_properties)
        MovieCompanyDF = pd.DataFrame(MovieCompany.collect(),columns=["movie_id", "company_id","indate"])
        if (len(MovieCompanyDF) > 0):
            LastDate = spark.read.jdbc(url=postgres_properties["url"],table="(select max(indate) as lastdate from movies.movie_company) t",properties=postgres_properties)
            LastDataDF = pd.DataFrame(LastDate.collect(),columns=["lastdate"])
            UpdateMetaData("movieCompany",LastDataDF["lastdate"][0])
            return MovieCompanyDF
        
    @task
    def MovieCrewPostgres():
        MovieCrew = spark.read.jdbc(url=postgres_properties["url"],table="(select * from movies.movie_crew where indate > (select lastdate from movies.meta_data where tablename = 'movieCrew')) t",properties=postgres_properties)
        MovieCrewDF = pd.DataFrame(MovieCrew.collect(),columns=["movie_id", "person_id", "department_id", "job","indate"])
        if (len(MovieCrewDF) > 0):
            LastDate = spark.read.jdbc(url=postgres_properties["url"],table="(select max(indate) as lastdate from movies.movie_crew) t",properties=postgres_properties)
            LastDataDF = pd.DataFrame(LastDate.collect(),columns=["lastdate"])
            UpdateMetaData("movieCrew",LastDataDF["lastdate"][0])
            return MovieCrewDF
        
    @task
    def MovieLanguagesPostgres():
        MovieLanguages = spark.read.jdbc(url=postgres_properties["url"],table="(select * from movies.movie_languages where indate > (select lastdate from movies.meta_data where tablename = 'movieLanguages')) t",properties=postgres_properties)
        MovieLanguagesDF = pd.DataFrame(MovieLanguages.collect(),columns=["movie_id", "language_id", "language_role_id","indate"])
        if (len(MovieLanguagesDF) > 0):
            LastDate = spark.read.jdbc(url=postgres_properties["url"],table="(select max(indate) as lastdate from movies.movie_languages) t",properties=postgres_properties)
            LastDataDF = pd.DataFrame(LastDate.collect(),columns=["lastdate"])
            UpdateMetaData("movieLanguages",LastDataDF["lastdate"][0])
            return MovieLanguagesDF
    
    @task
    def PersonPostgres():
        Person = spark.read.jdbc(url=postgres_properties["url"],table="(select * from movies.person where indate > (select lastdate from movies.meta_data where tablename = 'person')) t",properties=postgres_properties)
        PersonDF = pd.DataFrame(Person.collect(),columns=["person_id", "person_name","indate"])
        if (len(PersonDF) > 0):
            LastDate = spark.read.jdbc(url=postgres_properties["url"],table="(select max(indate) as lastdate from movies.person) t",properties=postgres_properties)
            LastDataDF = pd.DataFrame(LastDate.collect(),columns=["lastdate"])
            UpdateMetaData("person",LastDataDF["lastdate"][0])
            return PersonDF
        
    @task
    def ProductionCompanyPostgres():
        ProductionCompany = spark.read.jdbc(url=postgres_properties["url"],table="(select * from movies.production_company where indate > (select lastdate from movies.meta_data where tablename = 'productionCompany')) t",properties=postgres_properties)
        ProductionCompanyDF = pd.DataFrame(ProductionCompany.collect(),columns=["company_id", "company_name","indate"])
        if (len(ProductionCompanyDF) > 0):
            LastDate = spark.read.jdbc(url=postgres_properties["url"],table="(select max(indate) as lastdate from movies.production_company) t",properties=postgres_properties)
            LastDataDF = pd.DataFrame(LastDate.collect(),columns=["lastdate"])
            UpdateMetaData("productionCompany",LastDataDF["lastdate"][0])
            return ProductionCompanyDF
        
    @task
    def ProductionCountryPostgres():
        ProductionCountry = spark.read.jdbc(url=postgres_properties["url"],table="(select * from movies.production_country where indate > (select lastdate from movies.meta_data where tablename = 'productionCountry')) t",properties=postgres_properties)
        ProductionCountryDF = pd.DataFrame(ProductionCountry.collect(),columns=["movie_id", "country_id","indate"])
        if (len(ProductionCountryDF) > 0):
            LastDate = spark.read.jdbc(url=postgres_properties["url"],table="(select max(indate) as lastdate from movies.production_country) t",properties=postgres_properties)
            LastDataDF = pd.DataFrame(LastDate.collect(),columns=["lastdate"])
            UpdateMetaData("productionCountry",LastDataDF["lastdate"][0])
            return ProductionCountryDF
          
    @task
    def LoadIntoStagingArea(country , department , gender , genre , language , language_role , movie , movieGenre , movieCast, movieCompany, movieCrew, movieLanguages, person, production_company, production_country):
        countryDF = spark.createDataFrame(country)
        departmentDF = spark.createDataFrame(department)
        genderDF = spark.createDataFrame(gender)
        genreDF = spark.createDataFrame(genre)
        languageDF = spark.createDataFrame(language)
        languageroleDF = spark.createDataFrame(language_role)
        movieDF = spark.createDataFrame(movie)
        movieGenreDF = spark.createDataFrame(movieGenre)
        movieCastDF = spark.createDataFrame(movieCast)
        movieCompanyDF = spark.createDataFrame(movieCompany)
        movieCrewDF = spark.createDataFrame(movieCrew)
        movieLanguagesDF = spark.createDataFrame(movieLanguages)
        personDF = spark.createDataFrame(person)
        productionCompanyDF = spark.createDataFrame(production_company)
        productionCountryDF = spark.createDataFrame(production_country)

        countryDF.write.format("mongo").option("uri","mongodb://localhost:27017/MoviesStagingArea.country").mode("overwrite").save()
        departmentDF.write.format("mongo").option("uri","mongodb://localhost:27017/MoviesStagingArea.department").mode("overwrite").save()
        genderDF.write.format("mongo").option("uri","mongodb://localhost:27017/MoviesStagingArea.gender").mode("overwrite").save()
        genreDF.write.format("mongo").option("uri","mongodb://localhost:27017/MoviesStagingArea.genre").mode("overwrite").save()
        languageDF.write.format("mongo").option("uri","mongodb://localhost:27017/MoviesStagingArea.language").mode("overwrite").save()
        languageroleDF.write.format("mongo").option("uri","mongodb://localhost:27017/MoviesStagingArea.language_role").mode("overwrite").save()
        movieDF.write.format("mongo").option("uri","mongodb://localhost:27017/MoviesStagingArea.movie").mode("overwrite").save()
        movieGenreDF.write.format("mongo").option("uri","mongodb://localhost:27017/MoviesStagingArea.movie_genres").mode("overwrite").save()
        movieCastDF.write.format("mongo").option("uri","mongodb://localhost:27017/MoviesStagingArea.movie_cast").mode("overwrite").save()
        movieCompanyDF.write.format("mongo").option("uri","mongodb://localhost:27017/MoviesStagingArea.movie_company").mode("overwrite").save()
        movieCrewDF.write.format("mongo").option("uri","mongodb://localhost:27017/MoviesStagingArea.movie_crew").mode("overwrite").save()
        movieLanguagesDF.write.format("mongo").option("uri","mongodb://localhost:27017/MoviesStagingArea.movie_languages").mode("overwrite").save()
        personDF.write.format("mongo").option("uri","mongodb://localhost:27017/MoviesStagingArea.person").mode("overwrite").save()
        productionCompanyDF.write.format("mongo").option("uri","mongodb://localhost:27017/MoviesStagingArea.production_company").mode("overwrite").save()
        productionCountryDF.write.format("mongo").option("uri","mongodb://localhost:27017/MoviesStagingArea.production_country").mode("overwrite").save()   


    trigger_Talend_ETL = TriggerDagRunOperator(
        task_id='Trigger_Talend_ETL',
        trigger_dag_id='Talend_ETL',
    )

    country = CountryJson()
    department = DepartmentJson()
    gender = GenderXml()
    genre = GenreXml()
    language = LangaugeCsv()
    language_role = LanguageRoleCsv()
    movie = MoviePostgres()
    movieGenre = MovieGenrePostgres()
    movieCast = MovieCastPostgres()
    movieCompany = MovieCompanyPostgres()
    movieCrew = MovieCrewPostgres()
    movieLanguages = MovieLanguagesPostgres()
    person = PersonPostgres()
    productionCompany = ProductionCompanyPostgres()
    productionCountry = ProductionCountryPostgres()
    LoadIntoStaging = LoadIntoStagingArea( country , department , gender , genre , language , language_role , movie , movieGenre , movieCast , movieCompany ,
    movieCrew , movieLanguages , person , productionCompany , productionCountry )
    
    LoadIntoStaging >> trigger_Talend_ETL

MoviesStagingArea_ETL()
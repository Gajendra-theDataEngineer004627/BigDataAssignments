import os
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

# Replace 'C:/Python310/python.exe' with the correct Python executable path
os.environ['PYSPARK_PYTHON'] = 'C:/Python310/python.exe'
os.environ['HADOOP_HOME'] = 'C:/hadoop_bin'  # Make sure the path is correctly set

spark = SparkSession.builder \
    .appName("Assignemnt10") \
    .master("local[*]") \
    .getOrCreate()
"""
Below way is also useful if wanted to use the spark config with the spark session  
conf = SparkConf().setAppName("Assignemnt10").setMaster("local[*]")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
"""

sc = spark.sparkContext

# files_path = "C:/Users/Administrator/Downloads/Week10_Assignment/assignment_dataset/"

views_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("chapterId", IntegerType(), False),
    StructField("dateAndTime", TimestampType(), True)
])

title_schema = StructType([
    StructField("courseId",IntegerType(),False),
    StructField("title", StringType(), True)
])

chapter_schema = StructType([
    StructField("chapterId", IntegerType()),
    StructField("courseId", IntegerType())
])

view_p1 ="C:/Users/Administrator/Downloads/Week10_Assignment/assignment_dataset/views1-201108-004545.csv"
view_p2 ="C:/Users/Administrator/Downloads/Week10_Assignment/assignment_dataset/views2-201108-004545.csv"
view_p3 = "C:/Users/Administrator/Downloads/Week10_Assignment/assignment_dataset/views3-201108-004545.csv"
views_file_path = [view_p1,view_p2,view_p3]

v1 = spark.read.format("csv").schema(views_schema).option("path",view_p1).load()
v2 = spark.read.format("csv").schema(views_schema).option("path",view_p2).load()
v3 = spark.read.format("csv").schema(views_schema).option("path",view_p3).load()
views = v1.union(v2).union(v3)  # views Dataframe

title_path = "C:/Users/Administrator/Downloads/Week10_Assignment/assignment_dataset/titles-201108-004545.csv"
title_df = spark.read.format("csv").schema(title_schema).option("path",title_path).load()

chapters_path = "C:/Users/Administrator/Downloads/Week10_Assignment/assignment_dataset/chapters-201108-004545.csv"
chapters_df = spark.read.format("csv").schema(chapter_schema).option("path",chapters_path).load()

#views.show()

views1 = views
#views1.show(truncate=False)
#title_df.show()
titles = title_df
#titles.show(truncate=False)
#chapters_df.show()

# Exercise 1 : Find Out how many Chapters are there per course.
# Add on we will include titles of the course as well


chapters_per_course1 = chapters_df.join(titles, on="courseId", how="left") \
                                  .groupBy("courseId", "title") \
                                  .agg({"courseId": "count"}) \
                                  .withColumnRenamed("count(courseId)", "chapter_count") \
                                  .orderBy("courseId")


chapters_per_course1.show(truncate = False)
chapters_per_course1.createOrReplaceTempView("chapters_per_course_table")


#-----------------Solution for the Exercise 2 & 3-------------------------------------------#
# Exercise 2
# Your job is to produce a ranking chart detailing based on which are the most popular courses by score.
# I think now I have to use the Spark SQL for better analysis
chapters_df.createOrReplaceTempView("chapters_table")
titles.createOrReplaceTempView("title_table")
views.createOrReplaceTempView("view_table")

q1 = """select * from view_table limit 5 """
q2 = """ select * from title_table limit 5 """
q3 = """ select * from chapters_table limit 5"""
t1 = spark.sql(q1)
t3 = spark.sql(q3)
t2 = spark.sql(q2)

#t1.show()
#t2.show()
#t3.show()

test1 = """
with CTE_UserTableMod as (
select 
A.userId, A.chapterId,
A.dateAndTime as date,
B.courseId , C.title,
D.chapter_count,
ROW_NUMBER() over(partition by A.userId,A.chapterId,B.courseId order by A.dateAndTime) as row_num1   
from view_table A
left join chapters_table B
on A.chapterId = B.chapterId
left join title_table C  on B.courseID = C.courseId
left join chapters_per_course_table D on B.courseId = D.courseId and C.title = D.title )
select 
userId, chapterId,
date,courseId , title,
chapter_count,
row_num1
from CTE_UserTableMod
where row_num1 = 1
order by userID,courseId, chapterId
"""

tes = spark.sql(test1).createOrReplaceTempView("User_viewd_wrtCourse_table")

# ---------To verify Intermediate result Check the following comented command----------- #
#spark.sql(test1).show()

f1 = """
with CTE_Ftest as (
select 
userId,chapterId,courseId,title,chapter_count,
count(courseId) over(partition by courseId,userId) as completedChapters
from User_viewd_wrtCourse_table 
where row_num1 = 1
 )
select distinct
userId,
--chapterId,
courseId,title,
completedChapters,
chapter_count,
((completedChapters*100)/chapter_count) as course_completion_percent
from  CTE_Ftest
--where userId = 11 
order by courseId  --, chapterId
 """

# ---------To verify Intermediate result Check the following comented command----------- #
# spark.sql(f1).show()

spark.sql(f1).createOrReplaceTempView("User_with_coursecompletion_percentage")

test_query = """
select 
userId,
courseId,title,
completedChapters,
chapter_count,
course_completion_percent,
case 
when course_completion_percent >= 90.00 then 10
when course_completion_percent >= 50.00 and course_completion_percent < 90.00 then 4
when course_completion_percent >= 25.00 and course_completion_percent < 50.00 then 2
else 0
end as course_score
from User_with_coursecompletion_percentage
--where userId = 11 or userId = 12 
order by  userId,courseId
"""
spark.sql(test_query).createOrReplaceTempView("Final_table")
# For testing use some userIds for certain reasons

# ---------To verify Intermediate result Check the following comented command----------- #

#spark.sql(test_query).show(36)

# final Exercise solution both Exercise 2 and Exercise3 both are included

test_query1 = """
with CTE_final as (
select 
title,courseId,
sum(course_score) over(partition by courseId) as Score
from Final_table
)
select distinct
title,courseId,Score
from CTE_final
order by Score  DESC
"""

# for Exercise 2 and 3 solution
spark.sql(test_query1).show(truncate=False)
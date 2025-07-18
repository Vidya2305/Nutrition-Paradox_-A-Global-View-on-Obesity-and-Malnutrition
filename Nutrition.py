import pymysql
import pandas as pd
connection = pymysql.connect(
    host="localhost",
    user="root",
    password="********",
    database="Nutrition"
)
cursor = connection.cursor()

connection.commit()

#Query Writing(PowerBI)
queries = {
#Obesity Table (10 Queries)
#1.	Top 5 regions with the highest average obesity levels in the most recent year(2022)
    "query1_top5_regions_2022.csv": '''
    SELECT Region, AVG(Mean_Estimate) AS Avg_Obesity
    FROM obesity
    WHERE Year = 2022
    GROUP BY Region
    ORDER BY Avg_Obesity DESC
    LIMIT 5;
    ''',
#2. Top 5 countries with highest obesity estimates
    "query2_top5_countries.csv": '''
        SELECT Country, MAX(Mean_Estimate) AS Max_Obesity
        FROM obesity
        GROUP BY Country
        ORDER BY Max_Obesity DESC
        LIMIT 5;
    ''',
#3.	Obesity trend in India over the years(Mean_estimate)
    "query3_obesity_trend_india.csv": '''
    SELECT Year, AVG(Mean_Estimate) AS India_Obesity
    FROM obesity
    WHERE Country = 'India'
    GROUP BY Year
    ORDER BY Year;
    ''',
#4.	Average obesity by gender
    "query4_avg_obesity_by_gender.csv": '''
    SELECT Gender, AVG(Mean_Estimate) AS Avg_Obesity
    FROM obesity
    GROUP BY Gender;
    ''',
#5.	Country count by obesity level category and age group
    "query5_country_count_by_level_age.csv": '''
    SELECT Obesity_level, Age_group, COUNT(DISTINCT Country) AS Country_Count
    FROM obesity
    GROUP BY Obesity_level, Age_group;
    ''',
#6.	Top 5 countries least reliable countries(with highest CI_Width) and Top 5 most consistent countries (smallest average CI_Width)
    "query6_top5_least_reliable_countries.csv": '''
    SELECT Country, AVG(CI_Width) AS Avg_CI_Width
    FROM obesity
    GROUP BY Country
    ORDER BY Avg_CI_Width DESC
    LIMIT 5;
    ''',
    "query6_top5_most_consistent_countries.csv": '''
    SELECT Country, AVG(CI_Width) AS Avg_CI_Width
    FROM obesity
    GROUP BY Country
    ORDER BY Avg_CI_Width ASC
    LIMIT 5;
    ''',
#7.	Average obesity by age group
    "query7_avg_obesity_by_age.csv": '''
    SELECT Age_group, AVG(Mean_Estimate) AS Avg_Obesity
    FROM obesity
    GROUP BY Age_group;
    ''',
#8.	Top 10 Countries with consistent low obesity (low average + low CI)over the years

    "query8_top10_consistent_low_obesity.csv": '''
     SELECT Country, AVG(Mean_Estimate) AS Avg_Obesity, AVG(CI_Width) AS Avg_CI
     FROM obesity
     GROUP BY Country
     HAVING Avg_Obesity < 4.75 AND Avg_CI < 2.9
     ORDER BY Avg_Obesity ASC
     LIMIT 10;
     ''',
#9.	Countries where female obesity exceeds male by large margin (same year)
    "query9_female_exceeds_male.csv": '''
    SELECT f.Country, f.Year, f.Mean_Estimate - m.Mean_Estimate AS Difference
    FROM obesity f
    JOIN obesity m ON f.Country = m.Country AND f.Year = m.Year
    WHERE f.Gender = 'Female' AND m.Gender = 'Male' AND (f.Mean_Estimate - m.Mean_Estimate) > 5
    ORDER BY Difference DESC;
    ''',
#10.Global average obesity percentage per year
    "query10_global_avg_obesity_per_year.csv": '''
    SELECT Year, AVG(Mean_Estimate) AS Global_Obesity
    FROM obesity
    GROUP BY Year
    ORDER BY Year;
    ''',
# Malnutrition Table (10 Queries)
#1.	Avg. malnutrition by age group
    "query11_avg_malnutrition_by_age.csv": '''
    SELECT Age_group, AVG(Mean_Estimate) AS Avg_Malnutrition
    FROM malnutrition
    GROUP BY Age_group;
    ''',
#2.	Top 5 countries with highest malnutrition(mean_estimate)
    "query12_top5_countries_highest_malnutrition.csv": '''
    SELECT Country, MAX(Mean_Estimate) AS Max_Malnutrition
    FROM malnutrition
    GROUP BY Country
    ORDER BY Max_Malnutrition DESC
    LIMIT 5;
    ''',
#3.	Malnutrition trend in African region over the years
    "query13_africa_malnutrition_trend.csv": '''
    SELECT Year, AVG(Mean_Estimate) AS Africa_Trend
    FROM malnutrition 
    WHERE Region = 'Africa'
    GROUP BY Year
    ORDER BY Year;
    ''',
#4.	Gender-based average malnutrition
    "query14_avg_malnutrition_by_gender.csv": '''
    SELECT Gender, AVG(Mean_Estimate) AS Avg_Malnutrition
    FROM malnutrition
    GROUP BY Gender;
    ''',
#5.	Malnutrition level-wise (average CI_Width by age group)
    "query15_ci_width_by_level_age.csv": '''
    SELECT Malnutrition_level, Age_group, AVG(CI_Width) AS Avg_CI
    FROM malnutrition
    GROUP BY Malnutrition_level, Age_group;
    ''',
#6.	Yearly malnutrition change in specific countries(India, Nigeria, Brazil)
    "query16_yearly_change_in_key_countries.csv": '''
    SELECT Country, Year, AVG(Mean_Estimate) AS Avg_Estimate
    FROM malnutrition
    WHERE Country IN ('India', 'Nigeria', 'Brazil')
    GROUP BY Country, Year
    ORDER BY Country, Year;
    ''',
#7.	Regions with lowest malnutrition averages
    "query17_lowest_avg_malnutrition_regions.csv": '''
    SELECT Region, AVG(Mean_Estimate) AS Avg_Malnutrition
    FROM malnutrition
    GROUP BY Region
    ORDER BY Avg_Malnutrition ASC
    LIMIT 5;
    ''',
#8. Countries with increasing malnutrition
# (💡 Hint: Use MIN() and MAX() on Mean_Estimate per country to compare early vs. recent malnutrition levels,
    # and filter where the difference is positive using HAVING.)
    "query18_increasing_malnutrition_countries.csv": '''
    SELECT Country, MAX(Mean_Estimate) - MIN(Mean_Estimate) AS Increase
    FROM malnutrition
    GROUP BY Country
    HAVING Increase > 0
    ORDER BY Increase DESC
    LIMIT 10;
    ''',
#9. Min/Max malnutrition levels year-wise comparison
    "query19_min_max_malnutrition_yearly.csv": '''
    SELECT Year, MIN(Mean_Estimate) AS Min_Malnutrition, MAX(Mean_Estimate) AS Max_Malnutrition
    FROM malnutrition
    GROUP BY Year
    ORDER BY Year;
    ''',
#10.High CI_Width flags for monitoring(CI_width > 5)
    "query20_high_ci_monitoring_flags.csv": '''
    SELECT *
    FROM malnutrition
    WHERE CI_Width > 5;
    ''',
# Combined (5 Queries)
#1. Obesity vs malnutrition comparison by country(any 5 countries)
    "query21_compare_obesity_malnutrition.csv": '''
    SELECT o.Country, AVG(o.Mean_Estimate) AS Obesity, AVG(m.Mean_Estimate) AS Malnutrition
    FROM obesity o
    JOIN malnutrition m ON o.Country = m.Country AND o.Year = m.Year
    WHERE o.Country IN ('India', 'Nigeria', 'Brazil', 'United States', 'Indonesia')
    GROUP BY o.Country;
    ''',
#2. Gender-based disparity in both obesity and malnutrition
    "query22_gender_disparity_both.csv": '''
    SELECT o.Gender, AVG(o.Mean_Estimate) AS Avg_Obesity, AVG(m.Mean_Estimate) AS Avg_Malnutrition
    FROM obesity o
    JOIN malnutrition m ON o.Gender = m.Gender AND o.Country = m.Country AND o.Year = m.Year
    GROUP BY o.Gender;
    ''',
#3.	Region-wise avg estimates side-by-side(Africa and America)
    "query23_regionwise_avg_africa_america.csv": '''
    SELECT Region, AVG(Mean_Estimate) AS Avg_Estimate, 'Obesity' AS Source
    FROM obesity
    WHERE Region IN('Africa', 'Americas Region')
    GROUP BY Region
    UNION ALL
    SELECT Region, AVG(Mean_Estimate), 'Malnutrition'
    FROM malnutrition
    WHERE Region IN ('Africa', 'Americas Region')
    GROUP BY Region;
    ''',
#4. Countries with obesity up & malnutrition down
    "query24_obesity_up_malnutrition_down.csv": '''
    SELECT o.Country, AVG(o.Mean_Estimate) AS Obesity, AVG(m.Mean_Estimate) AS Malnutrition
    FROM obesity o
    JOIN malnutrition m ON o.Country = m.Country AND o.Year = m.Year
    GROUP BY o.Country
    HAVING Obesity > 20 AND Malnutrition < 3
    ORDER BY Obesity DESC;
    ''',
#5. Age-wise trend analysis
    "query25_age wise_trend_analysis.csv": '''
    SELECT o.Age_group, o.Year, AVG(o.Mean_Estimate) AS Avg_Obesity, AVG(m.Mean_Estimate) AS Avg_Malnutrition
    FROM obesity o
    JOIN malnutrition m ON o.Age_group = m.Age_group AND o.Country = m.Country AND o.Year = m.Year
    GROUP BY o.Age_group, o.Year
    ORDER BY o.Age_group, o.Year;
    ''',
# obesity
    "query26_obesity.csv": '''
    SELECT *
    FROM obesity;
    ''',
# malnutrition
    "query27_malnutrition.csv": '''
    SELECT *
    FROM malnutrition;
    '''
}

for filename, sql in queries.items():
    df = pd.read_sql(sql, connection)
    df.to_csv(filename, index=False)
    print("Saved: {filename}")




import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import sqlite3
import numpy as np

def boston_housing_import(file_loc, years_to_keep=[]):
    """Takes in a file location and a list of years to keep, and imports the boston housing data into a sqlite database.

    Takes about 2 minutes to run on my computer - this depends on your internet connection and computer speed.

    TODO: ZIP code's first 0 gets torn off. No real way to fix this as it's a probem with the df.to_sql() function.
    TODO: make years_to_keep more efficient; currently brings in all years and then filters out the ones we don't want.

    Args:
        file_loc (str): the location of the sqlite database. Will create a new one if it doesn't exist.
        years_to_keep (list, optional): The list of years to keep in the db. Defaults to [] (all years).
    """

    #assert that file_loc ends in.db
    assert file_loc.endswith('.db'), 'file_loc must end in .db'

    #assert that years_to_keep has only numbers from 2004-2023 or is empty
    assert all([year in range(2004, 2024) for year in years_to_keep]) or years_to_keep == [], 'years_to_keep must be empty or a list of numbers from 2004-2023'

    conn = sqlite3.connect(file_loc)
    # Get the url and parse it with BeautifulSoup
    url = 'https://data.boston.gov/dataset/property-assessment'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all anchor tags with href attribute (href is links)
    all_links = soup.find_all('a', href=True)

    # Filter links ending in .txt and .csv 
    txt_csv_links = [link['href'] for link in all_links if re.search(r'\.(txt|csv)$', link['href'])]

    # rename .txt into .csv, and create a dictionary with year as key and link as value
    years = list(range(2023, 2003, -1))
    link_dict = {year: (link[:-4] + '.csv' if link.endswith('.txt') else link) for year, link in zip(years, txt_csv_links)}

    for year in years:
        # read csv from boston's website into pandas dataframe (note: will fail if you don't have internet connection or if boston changes their website)
        df = pd.read_csv(link_dict[year], low_memory=False)

        table_name = str(year)

        # renames zipcode to be consistent (and to hopefully fix the drop 0 bug (it didnt))
        # all other renamings are done in the sql query below
        if year==2023:
            df["zipcode"] = df["ZIP_CODE"].astype(str).str.zfill(5)
            df = df.drop(columns=["ZIP_CODE"])


        #they decided to use dollar signs and commas for the land and building values in 2021 and only 2021
        if year==2021:
            for col in ["LAND_VALUE", "BLDG_VALUE"]:
                df[col] = df[col].str[1:]
                df[col] = df[col].str.replace(',', '')
                df[col] = df[col].str.strip()
                df[col] = df[col].replace([np.nan, np.inf, -np.inf], 0)
                df[col] = df[col].astype(float).astype(int)


        # write dataframe to sqlite database
        df.to_sql(table_name, conn, index=False, if_exists='replace', dtype={"zipcode": "text"})


    cur = conn.cursor()

    sql_query = """

    -- -----------------
    -- CLEAN TABLES
    -- -----------------

    -- renaming all the columns to be consistent

    ALTER TABLE '2014'
        RENAME COLUMN "Parcel_ID" TO PID;
    ALTER TABLE '2007'
        RENAME COLUMN "ST_NUM_CHAR" TO st_num;
    ALTER TABLE '2004'
        RENAME COLUMN "ST_NAME_SFX" TO ST_NAME_SUF;
    ALTER TABLE '2005'
        RENAME COLUMN "ST_NAME_SFX" TO ST_NAME_SUF;
    ALTER TABLE '2006'
        RENAME COLUMN "ST_SFX" TO ST_NAME_SUF;
    ALTER TABLE '2004'
        RENAME COLUMN "lotsize" TO land_sf;
    ALTER TABLE '2005'
        RENAME COLUMN "lotsize" TO land_sf;
    ALTER TABLE '2006'
        RENAME COLUMN "lotsize" TO land_sf;
    ALTER TABLE '2007'
        RENAME COLUMN "lotsize" TO land_sf;

    -- combining res_floor and cd_floor into one column

    ALTER TABLE '2023'
        ADD COLUMN NUM_FLOORS INTEGER;
    UPDATE '2023'
        SET NUM_FLOORS =
            CASE
                WHEN RES_FLOOR IS NULL OR RES_FLOOR = 'none' OR RES_FLOOR = 'null' THEN 0
                ELSE CAST(RES_FLOOR AS INTEGER)
            END +
            CASE
                WHEN CD_FLOOR IS NULL OR CD_FLOOR = 'none' OR CD_FLOOR = 'null' THEN 0
                ELSE CAST(CD_FLOOR AS INTEGER)
            END;


    ALTER TABLE '2022'
        ADD COLUMN NUM_FLOORS INTEGER;
    UPDATE '2022'
        SET NUM_FLOORS =
            CASE
                WHEN RES_FLOOR IS NULL OR RES_FLOOR = 'none' OR RES_FLOOR = 'null' THEN 0
                ELSE CAST(RES_FLOOR AS INTEGER)
            END +
            CASE
                WHEN CD_FLOOR IS NULL OR CD_FLOOR = 'none' OR CD_FLOOR = 'null' THEN 0
                ELSE CAST(CD_FLOOR AS INTEGER)
            END;


    ALTER TABLE '2021'
        ADD COLUMN NUM_FLOORS INTEGER;
    UPDATE '2021'
        SET NUM_FLOORS =
            CASE
                WHEN RES_FLOOR IS NULL OR RES_FLOOR = 'none' OR RES_FLOOR = 'null' THEN 0
                ELSE CAST(RES_FLOOR AS INTEGER)
            END +
            CASE
                WHEN CD_FLOOR IS NULL OR CD_FLOOR = 'none' OR CD_FLOOR = 'null' THEN 0
                ELSE CAST(CD_FLOOR AS INTEGER)
            END;


    -- rename all the land value columns to be consistent (generated these in excel)
    ALTER TABLE '2023' RENAME COLUMN "LAND_VALUE" TO LV_2023;
    ALTER TABLE '2022' RENAME COLUMN "LAND_VALUE" TO LV_2022;
    ALTER TABLE '2021' RENAME COLUMN "LAND_VALUE" TO LV_2021;
    ALTER TABLE '2020' RENAME COLUMN "AV_LAND" TO LV_2020;
    ALTER TABLE '2019' RENAME COLUMN "AV_LAND" TO LV_2019;
    ALTER TABLE '2018' RENAME COLUMN "AV_LAND" TO LV_2018;
    ALTER TABLE '2017' RENAME COLUMN "AV_LAND" TO LV_2017;
    ALTER TABLE '2016' RENAME COLUMN "AV_LAND" TO LV_2016;
    ALTER TABLE '2015' RENAME COLUMN "AV_LAND" TO LV_2015;
    ALTER TABLE '2014' RENAME COLUMN "AV_LAND" TO LV_2014;
    ALTER TABLE '2013' RENAME COLUMN "AV_LAND" TO LV_2013;
    ALTER TABLE '2012' RENAME COLUMN "AV_LAND" TO LV_2012;
    ALTER TABLE '2011' RENAME COLUMN "AV_LAND" TO LV_2011;
    ALTER TABLE '2010' RENAME COLUMN "AV_LAND" TO LV_2010;
    ALTER TABLE '2009' RENAME COLUMN "AV_LAND" TO LV_2009;
    ALTER TABLE '2008' RENAME COLUMN "FY2008_LAND" TO LV_2008;
    ALTER TABLE '2007' RENAME COLUMN "FY2007_LAND" TO LV_2007;
    ALTER TABLE '2006' RENAME COLUMN "FY2006_LAND" TO LV_2006;
    ALTER TABLE '2005' RENAME COLUMN "FY200_ LAND" TO LV_2005;
    ALTER TABLE '2004' RENAME COLUMN "FY200_ LAND" TO LV_2004;


    -- rename all the building value columns to be consistent (generated these in excel)

    ALTER TABLE '2023' RENAME COLUMN "BLDG_VALUE" TO BV_2023;
    ALTER TABLE '2022' RENAME COLUMN "BLDG_VALUE" TO BV_2022;
    ALTER TABLE '2021' RENAME COLUMN "BLDG_VALUE" TO BV_2021;
    ALTER TABLE '2020' RENAME COLUMN "AV_BLDG" TO BV_2020;
    ALTER TABLE '2019' RENAME COLUMN "AV_BLDG" TO BV_2019;
    ALTER TABLE '2018' RENAME COLUMN "AV_BLDG" TO BV_2018;
    ALTER TABLE '2017' RENAME COLUMN "AV_BLDG" TO BV_2017;
    ALTER TABLE '2016' RENAME COLUMN "AV_BLDG" TO BV_2016;
    ALTER TABLE '2015' RENAME COLUMN "AV_BLDG" TO BV_2015;
    ALTER TABLE '2014' RENAME COLUMN "AV_BLDG" TO BV_2014;
    ALTER TABLE '2013' RENAME COLUMN "AV_BLDG" TO BV_2013;
    ALTER TABLE '2012' RENAME COLUMN "AV_BLDG" TO BV_2012;
    ALTER TABLE '2011' RENAME COLUMN "AV_BLDG" TO BV_2011;
    ALTER TABLE '2010' RENAME COLUMN "AV_BLDG" TO BV_2010;
    ALTER TABLE '2009' RENAME COLUMN "AV_BLDG" TO BV_2009;
    ALTER TABLE '2008' RENAME COLUMN "FY2008_BLDG" TO BV_2008;
    ALTER TABLE '2007' RENAME COLUMN "FY2007_BLDG" TO BV_2007;
    ALTER TABLE '2006' RENAME COLUMN "FY2006_BLDG" TO BV_2006;
    ALTER TABLE '2005' RENAME COLUMN "FY2004_BLDG" TO BV_2005;
    ALTER TABLE '2004' RENAME COLUMN "FY2004_BLDG" TO BV_2004;

    -- split off the street suffix from the street name for certain years

    ALTER TABLE '2023' ADD COLUMN st_name_suf TEXT;
    UPDATE '2023' SET st_name_suf = SUBSTR(st_name, -2, 2);
    UPDATE '2023' SET st_name = SUBSTR(st_name, 1, LENGTH(st_name) - 3);

    ALTER TABLE '2022' ADD COLUMN st_name_suf TEXT;
    UPDATE '2022' SET st_name_suf = SUBSTR(st_name, -2, 2);
    UPDATE '2022' SET st_name = SUBSTR(st_name, 1, LENGTH(st_name) - 3);

    ALTER TABLE '2021' ADD COLUMN st_name_suf TEXT;
    UPDATE '2021' SET st_name_suf = SUBSTR(st_name, -2, 2);
    UPDATE '2021' SET st_name = SUBSTR(st_name, 1, LENGTH(st_name) - 3);


    -- -----------------
    -- CREATE TABLES
    -- -----------------

    DROP TABLE IF EXISTS buildings;
    CREATE TABLE Buildings(
        PID INT PRIMARY KEY,
        st_num INT,
        st_name VARCHAR(50),
        st_name_suf VARCHAR(2),
        zipcode INT,
        land_sf INT
    );

    INSERT OR IGNORE INTO Buildings (PID, st_num, st_name, st_name_suf, zipcode, land_sf)
    SELECT
        PID,
        st_num,
        st_name,
        st_name_suf,
        zipcode,
        land_sf
    FROM '2023';


    DROP TABLE IF EXISTS assessments;
    CREATE TABLE Assessments(
        PID INT,
        year INT,
        LV INT,
        BV INT,
        lu VARCHAR(2),
        gross_area FLOAT,
        num_floors INT,
        PRIMARY KEY (PID, year),
        FOREIGN KEY (PID) REFERENCES Buildings(PID)
    );

    INSERT OR IGNORE INTO Assessments (PID, year, LV, BV,lu,gross_area,num_floors)
    SELECT
        PID,
        2004 as year,
        LV_2004,
        BV_2004,
        lu,
        gross_area,
        num_floors
    FROM '2004';
    INSERT OR IGNORE INTO Assessments (PID, year, LV, BV,lu,gross_area,num_floors)
    SELECT
        PID,
        2005 as year,
        LV_2005,
        BV_2005,
        lu,
        gross_area,
        num_floors
    FROM '2005';
    INSERT OR IGNORE INTO Assessments (PID, year, LV, BV,lu,gross_area,num_floors)
    SELECT
        PID,
        2006 as year,
        LV_2006,
        BV_2006,
        lu,
        gross_area,
        num_floors
    FROM '2006';
    INSERT OR IGNORE INTO Assessments (PID, year, LV, BV,lu,gross_area,num_floors)
    SELECT
        PID,
        2007 as year,
        LV_2007,
        BV_2007,
        lu,
        gross_area,
        num_floors
    FROM '2007';
    INSERT OR IGNORE INTO Assessments (PID, year, LV, BV,lu,gross_area,num_floors)
    SELECT
        PID,
        2008 as year,
        LV_2008,
        BV_2008,
        lu,
        gross_area,
        num_floors
    FROM '2008';
    INSERT OR IGNORE INTO Assessments (PID, year, LV, BV,lu,gross_area,num_floors)
    SELECT
        PID,
        2009 as year,
        LV_2009,
        BV_2009,
        lu,
        gross_area,
        num_floors
    FROM '2009';
    INSERT OR IGNORE INTO Assessments (PID, year, LV, BV,lu,gross_area,num_floors)
    SELECT
        PID,
        2010 as year,
        LV_2010,
        BV_2010,
        lu,
        gross_area,
        num_floors
    FROM '2010';
    INSERT OR IGNORE INTO Assessments (PID, year, LV, BV,lu,gross_area,num_floors)
    SELECT
        PID,
        2011 as year,
        LV_2011,
        BV_2011,
        lu,
        gross_area,
        num_floors
    FROM '2011';
    INSERT OR IGNORE INTO Assessments (PID, year, LV, BV,lu,gross_area,num_floors)
    SELECT
        PID,
        2012 as year,
        LV_2012,
        BV_2012,
        lu,
        gross_area,
        num_floors
    FROM '2012';
    INSERT OR IGNORE INTO Assessments (PID, year, LV, BV,lu,gross_area,num_floors)
    SELECT
        PID,
        2013 as year,
        LV_2013,
        BV_2013,
        lu,
        gross_area,
        num_floors
    FROM '2013';
    INSERT OR IGNORE INTO Assessments (PID, year, LV, BV,lu,gross_area,num_floors)
    SELECT
        PID,
        2014 as year,
        LV_2014,
        BV_2014,
        lu,
        gross_area,
        num_floors
    FROM '2014';
    INSERT OR IGNORE INTO Assessments (PID, year, LV, BV,lu,gross_area,num_floors)
    SELECT
        PID,
        2015 as year,
        LV_2015,
        BV_2015,
        lu,
        gross_area,
        num_floors
    FROM '2015';
    INSERT OR IGNORE INTO Assessments (PID, year, LV, BV,lu,gross_area,num_floors)
    SELECT
        PID,
        2016 as year,
        LV_2016,
        BV_2016,
        lu,
        gross_area,
        num_floors
    FROM '2016';
    INSERT OR IGNORE INTO Assessments (PID, year, LV, BV,lu,gross_area,num_floors)
    SELECT
        PID,
        2017 as year,
        LV_2017,
        BV_2017,
        lu,
        gross_area,
        num_floors
    FROM '2017';
    INSERT OR IGNORE INTO Assessments (PID, year, LV, BV,lu,gross_area,num_floors)
    SELECT
        PID,
        2018 as year,
        LV_2018,
        BV_2018,
        lu,
        gross_area,
        num_floors
    FROM '2018';
    INSERT OR IGNORE INTO Assessments (PID, year, LV, BV,lu,gross_area,num_floors)
    SELECT
        PID,
        2019 as year,
        LV_2019,
        BV_2019,
        lu,
        gross_area,
        num_floors
    FROM '2019';
    INSERT OR IGNORE INTO Assessments (PID, year, LV, BV,lu,gross_area,num_floors)
    SELECT
        PID,
        2020 as year,
        LV_2020,
        BV_2020,
        lu,
        gross_area,
        num_floors
    FROM '2020';
    INSERT OR IGNORE INTO Assessments (PID, year, LV, BV,lu,gross_area,num_floors)
    SELECT
        PID,
        2021 as year,
        LV_2021,
        BV_2021,
        lu,
        gross_area,
        num_floors
    FROM '2021';
    INSERT OR IGNORE INTO Assessments (PID, year, LV, BV,lu,gross_area,num_floors)
    SELECT
        PID,
        2022 as year,
        LV_2022,
        BV_2022,
        lu,
        gross_area,
        num_floors
    FROM '2022';
    INSERT OR IGNORE INTO Assessments (PID, year, LV, BV,lu,gross_area,num_floors)
    SELECT
        PID,
        2023 as year,
        LV_2023,
        BV_2023,
        lu,
        gross_area,
        num_floors
    FROM '2023';


    DROP TABLE IF EXISTS '2004';
    DROP TABLE IF EXISTS '2005';
    DROP TABLE IF EXISTS '2006';
    DROP TABLE IF EXISTS '2007';
    DROP TABLE IF EXISTS '2008';
    DROP TABLE IF EXISTS '2009';
    DROP TABLE IF EXISTS '2010';
    DROP TABLE IF EXISTS '2011';
    DROP TABLE IF EXISTS '2012';
    DROP TABLE IF EXISTS '2013';
    DROP TABLE IF EXISTS '2014';
    DROP TABLE IF EXISTS '2015';
    DROP TABLE IF EXISTS '2016';
    DROP TABLE IF EXISTS '2017';
    DROP TABLE IF EXISTS '2018';
    DROP TABLE IF EXISTS '2019';
    DROP TABLE IF EXISTS '2020';
    DROP TABLE IF EXISTS '2021';
    DROP TABLE IF EXISTS '2022';
    DROP TABLE IF EXISTS '2023';
    """

    # Execute the SQL query
    cur.executescript(sql_query)

    if years_to_keep!=[]:
        # extremely roundabout way to get remove all years you don't want to keep
        years_str = ', '.join(map(str, years_to_keep))
        # delete rows where the year is not in the list
        cur.execute(f"DELETE FROM assessments WHERE year NOT IN ({years_str});")

    conn.commit()  

    cur.execute("VACUUM;")
    conn.commit()  

    conn.close()
## BCG - Big Data Case Study
---

To analyse the vehicle accidents across US

##### Dataset:

Data Set folder has 6 csv files.

##### Analytics:
##### Application should perform below analysis and store the results for each analysis.

Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?

Analysis 2: How many two-wheelers are booked for crashes?

Analysis 3: Which state has the highest number of accidents in which females are involved?

Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death

Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style

Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)

Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance

Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)



#### Project Structure

```
.
├── src
│   └── job
│   │   ├── __init__.py
│   │   └── men_dead_1.py
│   │   ├── two_wheeler_2.py
│   │   └── top_state_female_3.py
│   │   └── veh_make_injury_4.py
│   │   └── top_ethnic_group_5.py
│   │   └── top_zip_alcohol_6.py
│   │   └── no_prpt_damage_7.py
│   │   └── top_speeding_8.py
│   └── __init__.py
|   └── logger.py
│   └── main.py
│   └── utils.py
│   └── resources
│       ├── Charges_use.csv
│       └── Damages_use.csv
│       └── Endorse_use.csv
│       └── Primary_Person_use.csv
│       └── Restrict_use.csv
│       └── Units_use.csv
└── tests
|       └── __init__.py
|       └── test_files
|          └── test.csv
|       └── test_main.py
|       └── test_utils.py
├── config.json
├── README.md

```

#### Steps to run the analysis:

#### Input and Output file path

In this use case, Json file is used to configure the input and output files directory.

Update the input and output file path in the config file

### Execution Steps [Local]:

Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?

        spark-submit --master local[*] main.py --job men_dead_1
        
Analysis 2: How many two-wheelers are booked for crashes?
    
        spark-submit --master local[*] main.py --job two_wheeler_2

Analysis 3: Which state has the highest number of accidents in which females are involved?
        
        spark-submit --master local[*] main.py --job top_state_female_3   

Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death

        spark-submit --master local[*] main.py --job veh_make_injury_4

Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style

        spark-submit --master local[*] main.py --job top_ethnic_group_5

Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)

        spark-submit --master local[*] main.py --job top_zip_alcohol_6   

Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    
        spark-submit --master local[*] main.py --job no_prpt_damage_7

Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
    
        spark-submit --master local[*] main.py --job top_speeding_8


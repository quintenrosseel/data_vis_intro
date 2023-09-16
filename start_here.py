# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Data Exploration 
# MAGIC
# MAGIC ## Getting Started
# MAGIC The first questions to ask yourself when creating a data visualisation are; 
# MAGIC
# MAGIC **1. What is the message I want to convey with this report?**
# MAGIC - What is the data story? (see tips in the [Miro board](https://miro.com/app/board/uXjVMoBkTRQ=/?share_link_id=950119977618))
# MAGIC - What do I want to highlight? (explore your dataset)
# MAGIC
# MAGIC **2. Who is the audience for this report?**
# MAGIC - C-level vs operational people
# MAGIC
# MAGIC **3. What type of visualisation or report is it?**
# MAGIC - Exploratory data visualisaton
# MAGIC - Narrative data visualisation
# MAGIC - Story templates / story system in the Miro board
# MAGIC
# MAGIC Often times, you don't know your dataset, so we need to explore it first.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Set up the data environment
# MAGIC
# MAGIC Pandas and numpy come out-of-the box on Databricks.
# MAGIC To advance our workflow, let's download a handy tool to understand our dataset better, [Ydata Profiling](https://github.com/ydataai/ydata-profiling)! 

# COMMAND ----------

!pip install ydata-profiling

# COMMAND ----------

import pandas as pd
import numpy as np
import ydata_profiling

# COMMAND ----------

# MAGIC %md 
# MAGIC **Adding your dataset**
# MAGIC - Execute the command below to get information such as `YOUR_EMAIL`, `YOUR_REPO_NAME`. 
# MAGIC - Add your dataset in the `data` directory using the `import` button in the UI. 

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Reading in the dataset
# MAGIC Make sure to fill in `YOUR_EMAIL` & `YOUR_REPO_NAME` to ensure that Databricks reads in the file correctly. 

# COMMAND ----------

# Configuration
YOUR_EMAIL = 'rossequ@cronos.be'
YOUR_REPO_NAME = 'data_vis_intro' 
DATA_BASE_DIR = f'file:/Workspace/Repos/{YOUR_EMAIL}/{YOUR_REPO_NAME}/data' 

# All available datasets 
datasets = {
    "accidents": f"{DATA_BASE_DIR}/TF_ACCIDENTS_2021.csv", 
    "population": f"{DATA_BASE_DIR}/TF_SOC_POP_STRUCT_2021.csv"
}

accidents_2021_df = pd.read_csv(datasets['accidents'])
population_2021_df = pd.read_csv(datasets['population'])

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Common functions for data exploration 
# MAGIC - `df.info()`: Metadata information of your dataset
# MAGIC - `df.describe()`: Summary stats of your dataset
# MAGIC - `df.head(n)`: top `n` rows of your dataset
# MAGIC - `ydata_profiling.ProfileReport`: a full report of your dataset. This is what you'll use to inform your data story.  

# COMMAND ----------

accidents_2021_df.info()

# COMMAND ----------

accidents_2021_df.describe()

# COMMAND ----------

accidents_2021_df.head(5)

# COMMAND ----------

from ydata_profiling import ProfileReport

eda_report = ProfileReport(accidents_2021_df)

# Show the report
eda_report

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Refining your data story 
# MAGIC With the dataset below, we have a lot of information that can help us to create a juicy story. 
# MAGIC From here, you can drill down using libraries such as 
# MAGIC
# MAGIC - `matplotlib`: the go-to python library for data visualisation. 
# MAGIC - `seaborn`: an extension of matplotlib to create more appealing visualisations. 
# MAGIC - `Plotly`: the go-to python library for interactive plots. Note: Databricks has an inline integration of Plotly. 

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Example 
# MAGIC Refining your dataset for more comprehensive visualisations. 

# COMMAND ----------

# Example data transformations 

# Create a map for day of the week. 
days_of_week = {
    1: 'monday',
    2: 'tuesday',
    3: 'wednesday',
    4: 'thursday',
    5: 'friday',
    6: 'saturday',
    7: 'sunday'
}

def add_weekday_en(
        df: pd.DataFrame, 
        weekday_target_col: str = 'weekday', 
        weekday_source_col: str = 'CD_DAY_OF_WEEK', 
        map_dict = None) -> pd.DataFrame: 
    """
        Adds an english day of the week, using a dictionary map. 
    """
    df[weekday_target_col] = df[weekday_source_col].map(map_dict)
    return df 

# Create the visualisation dataset 
histogram_vis_df = (
    accidents_2021_df.copy(deep=True)
    # Add an english day of the week
    .pipe(add_weekday_en, map_dict=days_of_week)
    # Sort by day of the week
    .pipe(pd.DataFrame.sort_values, by=['CD_DAY_OF_WEEK'], ascending=True)
    # Groupby & Count number of accidents per day of the week. 
    .groupby(['weekday'], as_index=False, sort=False).count()
    # Rename columns to account for COUNT operation
    .pipe(pd.DataFrame.rename, columns={c: c + '_CNT' for c in accidents_2021_df.columns})
)   
display(histogram_vis_df)

# COMMAND ----------

# Now it's up to you. 
# Check your data to gain a better understanding and insight that you want to 

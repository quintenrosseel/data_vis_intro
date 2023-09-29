# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Cleaning Examples
# MAGIC

# COMMAND ----------

import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Reading in the dataset
# MAGIC
# MAGIC - Execute the command below to get information such as `YOUR_EMAIL`, `YOUR_REPO_NAME`. 
# MAGIC - Add your dataset in the `data` directory using the `import` button in the UI. 
# MAGIC - Make sure to fill in `YOUR_EMAIL` & `YOUR_REPO_NAME` to ensure that Databricks reads in the file correctly. 

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

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
# MAGIC # Common functions for data cleaning 
# MAGIC

# COMMAND ----------

# Create DataFrame
df = pd.DataFrame({
    'Name': ['Alice', 'Bob', 'Charlie', np.nan, 'Eve', 'Frank'],
    'Age': [25, 35, np.nan, 40, 19, 120],
    'Salary': ['50k', '40K', '60K', '80k', '30k', '100K'],
    'Comments': ['She is good', 'Not available', 'He is awesome', np.nan, 'Fresh Graduate', 'Exceptional Case']
})

df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## NLP Techniques for Dealing with Unstructured Data Transformations
# MAGIC
# MAGIC - Text normalization: 
# MAGIC - Tokenization: turn words into standardized tokens. 
# MAGIC - Vectorization: turn sentences into embeddings 

# COMMAND ----------

# Using simple Python functions for text normalization
df['Comments'] = df['Comments'].str.lower()

# Tokenization using split()
df['Comments'].str.split()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC These changes happen in-place. So watch out when you do this. 
# MAGIC I usually copy my source dataset, to always have my dataset available when a data transformation doesn't do what I want. 

# COMMAND ----------

def my_transformation(df: pd.DataFrame, col_name: str) -> pd.DataFrame: 
    """
        Run an arbitrary transformation on your dataframe. 
    """

    df[col_name] = df[col_name].str.lower()
    df[f"{col_name}_tokens"] = df['Comments'].str.split()
    return df


new_df = (
    df.copy(deep=True)
    .pipe(my_transformation, col_name="Comments")
)

new_df

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Data Quality: Data Unit Testing
# MAGIC
# MAGIC To ensure that you know what's happening, you better test your code as you go. 
# MAGIC - Check data types
# MAGIC - Validate against known constraints
# MAGIC - Run a dummy data set through your transformation, and ensure that it returns what it's supposed to return the same. 

# COMMAND ----------

# Check Data Types
assert df['Age'].dtype == 'float64'

# Validate Constraints (e.g., Age should be < 100) - validate that the lenght is still the same. 
assert df[df['Age'] < 100].shape[0] == df.shape[0]

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Data Observability
# MAGIC - Describe the data
# MAGIC - Visualize distributions

# COMMAND ----------

import matplotlib.pyplot as plt

# Descriptive statistics
print(df.describe())

# Histogram (Pandas has some inline plotting functions too)
df['Age'].plot(kind='hist')
plt.show()



# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Dealing with Outliers & Anomalies
# MAGIC - Z-score

# COMMAND ----------

from scipy import stats


def add_zcore(df: pd.DataFrame, col_name: str) -> pd.DataFrame: 
    """
        Add a z-score column to a DataFrame. 
    """
    # Calculate Z-score
    z_scores = np.abs(stats.zscore(df[col_name].dropna()))
    print("Z SCORE \n", z_scores)

    # Fill in column with nans for starter
    df[f'{col_name}_z_score'] = np.nan
    
    # Impute 
    df.loc[df[col_name].notna(), f'{col_name}_z_score'] = z_scores
    return df

# Remove outliers
z_df = (
    df.copy(deep=True)
    .pipe(add_zcore, col_name="Age")
) 

z_df.display()

# COMMAND ----------

z_df[
    (z_df['Age_z_score'].isna()) | 
    ((z_df['Age_z_score'] < 1.5))
]

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Inconsistent Values & Improperly Formatted Values
# MAGIC - String manipulation
# MAGIC - Case normalization

# COMMAND ----------

def transform_salary(df: pd.DataFrame) -> pd.DataFrame:
    """
    """
    # Convert Salary to lowercase
    df['Salary'] = df['Salary'].str.lower()

    return df


new_df = (
    df.copy(deep=True)
    .pipe(transform_salary)
)

new_df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Missing Records: Data Imputation
# MAGIC - Mean imputation
# MAGIC - K-NN imputation

# COMMAND ----------

def mean_impute(df: pd.DataFrame, col_name: str) -> pd.DataFrame: 
    """
        Impute the mean in the given col_name. 
    """
    df[col_name].fillna(df[col_name].mean(), inplace=True)
    return df

def drop_inplace(df:pd.DataFrame, col_name: str) -> pd.DataFrame: 
    """
        Drop rows where a given col_name is null.  
    """
    # Returns void
    df.dropna(subset=[col_name], inplace=True)
    return df

new_df = (
    df.copy(deep=True)
    .pipe(mean_impute, col_name='Age')
    .pipe(drop_inplace, col_name='Name')
)    

new_df.display()

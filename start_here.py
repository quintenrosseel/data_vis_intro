# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # About this notebook
# MAGIC
# MAGIC - This is a template to get you started with your data story. 
# MAGIC - The dataset is taken from Statbel, and involves Belgian accidents in 2021. 
# MAGIC - If you need inspiration for potential datasets, see [this link](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4567555).
# MAGIC
# MAGIC # Getting Started
# MAGIC The first questions to ask yourself when creating a data visualisation are listed below. 
# MAGIC These steps 
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
# MAGIC
# MAGIC Often times, you don't know your dataset, so we need to explore it first. From 

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC First, let's download a handy tool to understand our dataset better, [Ydata Profiling](https://github.com/ydataai/ydata-profiling)! 

# COMMAND ----------

!pip install ydata-profiling

# COMMAND ----------

import pandas as pd
import numpy as np
import ydata_profiling as ydp 

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
# MAGIC
# MAGIC # Data Exploration 

# COMMAND ----------

accidents_2021_df.head(20)

# COMMAND ----------

import matplotlib.pyplot as plt 
import matplotlib.animation 
import numpy as np

t = np. linspace(0, 10, 1000)
fig, ax = plt.subplots(1, 1, figsize=(16,9) , dpi=300)
ax. set_facecolor("black" )
ax.axis(False)
fig. set_facecolor("black")
line, = ax.plot(t, np.sin( frequency_values [0]*t), lw=5, color="white")
ax.set_ylim(-5,5)

def animate(playhead):
    mask = (t <= playhead)
    line.set_data(t[mask], np.sin(4*t [mask]))
    anim = matplotlib.animation. FuncAnimation( fig, animate, t[::101, interval=30)
    anim.save "Matplotlib simple wave.mp4" )


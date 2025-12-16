import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from extract_titanic import extract_data
import os

def transform_data(raw_path):
    base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir=os.path.join(base_dir,"data","staged")
    os.makedirs(staged_dir,exist_ok=True)
    df=pd.read_csv(raw_path)

    ##Handling missing values
    numerical_cols=["survived","parch","pclass","sibsp","age","fare",]

    #fill the missing values with median
    for col in numerical_cols:
        df[col]=df[col].fillna(df[col].median())
# Fill embarked missing values with mode
    cat_cols=df.select_dtypes('object').columns
    for col in cat_cols:
        df[col] = df[col].fillna(df[col].mode()[0])
# Drop rows with missing deck info
    df = df.dropna(subset=['deck'])

    #Feature engineering
    df["familysize"] = df["sibsp"] + df["parch"] + 1
    df["isalone"] = (df["familysize"] == 1).astype(int)

 
    
    ##Save the transformed data

    staged_path=os.path.join(staged_dir,"titanic_transformed.csv")
    df.to_csv(staged_path,index=False)

    print(f"Data transformed and saved at {staged_path}")

    return staged_path

if __name__=="__main__":
    path=extract_data()
    transform_data(path)

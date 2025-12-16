import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from extract_iris import extract_data
import os

def transform_data(raw_path):
    base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir=os.path.join(base_dir,"data","staged")
    os.makedirs(staged_dir,exist_ok=True)
    df=pd.read_csv(raw_path)

    ##Handling missing values
    numerical_cols=['sepal_length',"sepal_width","petal_length","petal_width"]

    #fill the missing values with median
    for col in numerical_cols:
        df[col]=df[col].fillna(df[col].median())

    #fill categorical data with mode value
    df["species"]=df["species"].fillna(df['species'].mode()[0])

    ##Feature engineering
    df["sepal_ratio"]=df["sepal_length"]/df["sepal_width"]

    df["petal_ratio"]=df["petal_length"]/df["petal_width"]

    df['is_petal_long']=(df["petal_length"]>df["petal_length"].median()).astype(int)

    ##Drop unnecessary columns      
    df.drop(columns=[],inplace=True,errors="ignore")

    ##Save the transformed data

    staged_path=os.path.join(staged_dir,"iris_transformed.csv")
    df.to_csv(staged_path,index=False)

    print(f"Data transformed and saved at {staged_path}")

    return staged_path

if __name__=="__main__":
    path=extract_data()
    transform_data(path)

import pandas as pd

measurement_data_dataframe = pd.read_csv("./BZ011_Rohdaten.dat", sep="\t", decimal=",")
print(measurement_data_dataframe.columns)
measurement_data_dataframe = measurement_data_dataframe[
    measurement_data_dataframe["Set Kommentar"] == "0,60V"
]

measurement_data_dataframe.to_csv("./small_BZ011_Rohdaten.dat", sep="\t", decimal=",")

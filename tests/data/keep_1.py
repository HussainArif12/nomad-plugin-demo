import pandas as pd

measurement_data_dataframe = pd.read_csv(
    "./BZ011_Rohdaten.dat", nrows=1, sep="\t", decimal=","
)
measurement_data_dataframe.to_csv("./small_BZ011_Rohdaten.dat", sep="\t", decimal=",")

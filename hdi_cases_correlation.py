import pandas as pd
from pandas import read_csv
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib
import numpy as np



data1 = read_csv("time_series_covid19_confirmed_global.csv",sep=",")
data1 = data1.drop(['Province/State', 'Lat','Long'], axis=1)
col_list= list(data1)
col_list.remove('Country/Region')
print(col_list)
data1['sum_of_cases'] = data1[col_list].sum(axis=1)

data1 = data1[['Country/Region','sum_of_cases']]


data2 = read_csv("time_series_covid19_deaths_global.csv",sep=",")
data2 = data2.drop(['Province/State', 'Lat','Long'], axis=1)
col_list= list(data2)
col_list.remove('Country/Region')
print(col_list)
data1['sum_of_deaths'] = data2[col_list].sum(axis=1)

data2 = data1[['Country/Region','sum_of_deaths']]

data3 = pd.merge(data1,data2,how="inner")

data_population = read_csv("population-figures-by-country-csv_csv.csv",sep=";")
data_population=data_population.rename({'Country': 'Country/Region'}, axis=1)
data_population_cases = pd.merge(data3,data_population,how="inner")
data_population_cases['cases_normalized'] = data_population_cases['sum_of_cases']/data_population_cases['Year_2016']
data_population_cases['deaths_normalized'] = data_population_cases['sum_of_deaths']/data_population_cases['Year_2016']




data_hdi = read_csv('human-development-index.csv',sep=",")
data_hdi = data_hdi[data_hdi['Year']==2017]
data_hdi = data_hdi[['Entity',' ((0-1; higher values are better))']]
print(data_hdi)


data_hdi_combination_cases_deaths = pd.merge(data_population_cases,data_hdi,how='inner',left_on=['Country/Region'],right_on=['Entity'])
data_hdi_combination_cases_deaths = data_hdi_combination_cases_deaths[['cases_normalized','deaths_normalized',' ((0-1; higher values are better))']]
print(data_hdi_combination_cases_deaths)




data_hdi_combination_cases_deaths = data_hdi_combination_cases_deaths.rename(columns={' ((0-1; higher values are better))': 'hdi'})
corr = data_hdi_combination_cases_deaths.corr()
mask = np.zeros_like(corr, dtype=np.bool)
mask[np.triu_indices_from(mask)] = True

# Set up the matplotlib figure


boundaries = [0, 0.1, 0.5, 0.7, 1]

hex_colors = sns.light_palette('purple', n_colors=len(boundaries) * 2 + 2, as_cmap=False).as_hex()
hex_colors = [hex_colors[i] for i in range(0, len(hex_colors), 2)]

colors=list(zip(boundaries, hex_colors))

custom_color_map = matplotlib.colors.LinearSegmentedColormap.from_list(
    name='custom_navy',
    colors=colors,
)

# Draw the heatmap with the mask and correct aspect ratio
ax = sns.heatmap(corr, mask=mask, cmap=custom_color_map, vmin = 0,xticklabels=corr.columns,
        yticklabels=corr.columns,annot=True,cbar_kws={"shrink": .5})
# Generate a mask for the upper triangle

bottom, top = ax.get_ylim()
ax.set_ylim(bottom + 0.5, top - 0.5)

plt.show()

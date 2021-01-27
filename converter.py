import pandas as pd

precipitations_mm_merignac = pd.read_html('data/gathered_from_internet/precipitations_mm_merignac.txt', header=0, index_col=0)[0]
print(precipitations_mm_merignac.head(5).to_string())

temp_maximals_merignac = pd.read_html('data/gathered_from_internet/temp_maximals_merignac.txt', header=0, index_col=0)[0]
temp_minimals_merignac = pd.read_html('data/gathered_from_internet/temp_minimals_merignac.txt', header=0, index_col=0)[0]
frost_days_count_merignac = pd.read_html('data/gathered_from_internet/frost_days_count_merignac.txt', header=0, index_col=0)[0]
insulation_merignac = pd.read_html('data/gathered_from_internet/insulation_merignac.txt', header=0, index_col=0)[0]


writer = pd.ExcelWriter('data/weather_merignac.xlsx')
precipitations_mm_merignac.to_excel(writer, 'precipitations_mm')
temp_maximals_merignac.to_excel(writer, 'temp_maximals')
temp_minimals_merignac.to_excel(writer, 'temp_minimals')
frost_days_count_merignac.to_excel(writer, 'frost_days_count')
insulation_merignac.to_excel(writer, 'insulation')
writer.save()



# %%
import pandas as pd
import services as services
from IPython.display import display

# %%
layout_file_path = "/tmp/b3/b3_historical_quotations_file_layout.csv"

df_layout = pd.read_csv(layout_file_path)
display(df_layout)

# %%




# %%
import pandas as pd
import camelot
import os
from IPython.display import display

pdf_path = "/tmp/b3/HistoricalQuotations_B3.pdf"
out_dir = "/tmp/b3"

# %%
tables = camelot.read_pdf(pdf_path, pages="all", flavor="lattice")
if tables.n == 0:
    tables = camelot.read_pdf(pdf_path, pages="all", flavor="stream")

df_layout = pd.DataFrame()
for idx, table in enumerate(tables, start=1):
    if idx in [1, 5]:
        continue
    values = table.df.values
    columns = ['field_description', 'content', 'size', 'init_pos', 'final_pos']
    df_table = pd.DataFrame(data=values[1:], columns=columns)
    df_layout = pd.concat([df_layout, df_table])
df_layout.reset_index(drop=True, inplace=True)
df_layout = (
    df_layout.astype(str)
    .replace(r"(\w)\n+(\w)", r"\1\2", regex=True)
    .replace(r"\n+", " ", regex=True)
    .replace(' OF EXCHANGE', '', regex=True)
    .replace(' - NEG.', '', regex=True)
)
df_layout[["field", "description"]] = (
    df_layout["field_description"]
    .str.split("–", n=1, expand=True)
    .apply(lambda col: col.str.strip())
)
df_layout = df_layout[['field', 'init_pos', 'final_pos']]
display(df_layout)

# %%
df_layout.to_csv(
    os.path.join(out_dir, "b3_historical_quotations_file_layout.csv"), 
    index=False)



# %%

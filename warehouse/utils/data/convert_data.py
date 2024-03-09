import yaml
import pandas as pd

from pathlib import Path

df = pd.read_csv("warehouse/utils/data/amazon_fba_fulfillment_center.csv")
df["zipcode"] = df["zipcode"].astype(str)
df = df.drop_duplicates(keep="first")
file_path = Path(__file__).parent.parent.resolve().joinpath("fba_fulfillment_center.yaml")
with open(file_path, "w") as f:
    documents = yaml.dump(
        df.set_index("fba_code").to_dict(orient="index"), 
        f, 
        default_flow_style=False
    )

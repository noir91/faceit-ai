import pandas as pd
import numpy as np

from matplotlib import pyplot as plt
import matplotlib

#matplotlib.use('TkAgg')


path = "response/response_details_18_Mar_2026_02_32AM.json"
response_dataframe = pd.read_json(path, orient = 'index')

stats = response_dataframe.iloc[-1].drop(['url', 'latency', 'status'])
latencies = response_dataframe.iloc[:-1]

x = range(len(latencies))
y = latencies["latency"]

plt.ylim(0, 1)
plt.figure(figsize=(10, 6))
plt.plot(x, y, marker='o')

plt.xlabel("Request Number")
plt.ylabel("Latency (seconds)")
plt.title("Latency per Request")
plt.grid()


plt.savefig("plot1.png")
plt.show()
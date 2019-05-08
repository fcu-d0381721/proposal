import pandas as pd
import matplotlib.pyplot as plot

avg_month = pd.read_csv('avg_month.csv', encoding='utf-8')


plot.bar(avg_month['sno'], avg_month['rent'], color='red', alpha=0.5, width=1, align="edge", label="rent")
plot.bar(avg_month['sno'], avg_month['return'], color='blue', alpha=0.5, width=-1, align="edge", label="return")
plot.legend(loc=2)
plot.autoscale(enable=True, axis='x', tight=True)
plot.xlabel('sno')
plot.ylabel('avg')
plot.show()
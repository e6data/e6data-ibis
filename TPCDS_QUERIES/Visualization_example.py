import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

import pandas as pd

from ibis.backends.e6data import Backend

# IDEA: Visualize Total Sales Over Time by Product Category.
# We are plotting the total sales (ss_sales_price) over different dates, grouped by product category (i_category), which could be useful for sales trend analysis.
con = Backend().do_connect(
    host='e6data-ibis-sl56milkae-3f8a70b917f58424.elb.us-east-1.amazonaws.com',
    port=80,
    username='kanamarlapudi@e6x.io',
    password='HLlGv3eob5ml8Ouy4FpkW7j75K7CBr8H1wYsyg0UpRbuazGuHZsl4lhi',
    database='tpcds_1000_delta',
    catalog_name='glue'
)

print("Connection is made!!")
# Step 2: Define tables
store_sales = con.table('store_sales', ('glue', 'tpcds_1000_delta'))
date_dim = con.table('date_dim', ('glue', 'tpcds_1000_delta'))
item = con.table('item', ('glue', 'tpcds_1000_delta'))

# Step 3: Construct the query with filtering for specific categories
query = (
    store_sales
    .join(date_dim, store_sales['ss_sold_date_sk'] == date_dim['d_date_sk'])
    .join(item, store_sales['ss_item_sk'] == item['i_item_sk'])
    .filter(item['i_category'].isin(['Books', 'Electronics', 'Jewelry']))  # Filter for selected categories
    .group_by([date_dim['d_date'], item['i_category']])
    .aggregate(total_sales=store_sales['ss_sales_price'].sum())
    .order_by(date_dim['d_date'])
    .limit(100)
)

# Step 4: Execute the query
df = query.execute()
print(df)

# Step 5: Visualize the result
df['total_sales'] = pd.to_numeric(df['total_sales'], errors='coerce')

# Pivot the data for plotting
pivot_df = df.pivot(index='d_date', columns='i_category', values='total_sales')

# Plot the data
pivot_df.plot(kind='line', figsize=(10, 6))

# Format the y-axis to display values in millions with custom ticks
plt.gca().yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'{x*1e-6:.2f}M'))

# Manually set y-axis ticks for finer control
plt.yticks([3.10e6, 3.15e6, 3.20e6, 3.25e6, 3.30e6])

# Set the title and labels
plt.title('Total Sales by Category Over Time')
plt.ylabel('Total Sales (in Millions)')
plt.xlabel('Date')

# Adding a legend with a better position and box style
plt.legend(title='Category', loc='upper left', bbox_to_anchor=(1.05, 1), fontsize=10)

# Rotate the x-axis labels for better readability
plt.xticks(rotation=45)

# Show the plot
plt.tight_layout()
plt.show()

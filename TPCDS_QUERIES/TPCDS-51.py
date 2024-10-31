import ibis

# Step 1: Connect to the e6data backend
from ibis.backends.e6data import Backend

con = Backend().do_connect(
    host='e6data-ibis-sl56milkae-3f8a70b917f58424.elb.us-east-1.amazonaws.com',
    port=80,
    username='kanamarlapudi@e6x.io',
    password='HLlGv3eob5ml8Ouy4FpkW7j75K7CBr8H1wYsyg0UpRbuazGuHZsl4lhi',
    database='tpcds_1000_delta',
    catalog_name='glue'
)
# Step 2: Define tables from the 'tpcds_1000_delta' database in 'glue' catalog
store_sales = con.table('store_sales', ('glue', 'tpcds_1000_delta'))
date_dim = con.table('date_dim', ('glue', 'tpcds_1000_delta'))
item = con.table('item', ('glue', 'tpcds_1000_delta'))

# Step 3: Define the main query using Ibis expressions
query = (
    store_sales
    .join(date_dim, store_sales['ss_sold_date_sk'] == date_dim['d_date_sk'])
    .join(item, store_sales['ss_item_sk'] == item['i_item_sk'])
    .filter(
        item['i_manager_id'] == 36,
        date_dim['d_moy'] == 12,
        date_dim['d_year'] == 2001
    )
    .group_by([item['i_brand'], item['i_brand_id']])
    .aggregate(
        ext_price=store_sales['ss_ext_sales_price'].sum()
    )
    .order_by([ibis.desc('ext_price'), item['i_brand_id']])
    .limit(100)
)

# Step 4: Execute the query and get the result as a Pandas DataFrame
df = query.execute()

# Step 5: Print the result
print(df)
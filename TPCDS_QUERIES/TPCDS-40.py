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
web_sales = con.table('web_sales', ('glue', 'tpcds_1000_delta'))
customer = con.table('customer', ('glue', 'tpcds_1000_delta'))
customer_address = con.table('customer_address', ('glue', 'tpcds_1000_delta'))
date_dim = con.table('date_dim', ('glue', 'tpcds_1000_delta'))
item = con.table('item', ('glue', 'tpcds_1000_delta'))

# Step 3: Define the subquery
# Subquery for filtering i_item_id
subquery_item_id = (
    item
    .filter(item['i_item_sk'].isin([2, 3, 5, 7, 11, 13, 17, 19, 23, 29]))
    .select(item['i_item_id'])
)

# Step 4: Modify the main query using an inner join instead of `isin`
query = (
    web_sales
    .join(customer, web_sales['ws_bill_customer_sk'] == customer['c_customer_sk'])
    .join(customer_address, customer['c_current_addr_sk'] == customer_address['ca_address_sk'])
    .join(item, web_sales['ws_item_sk'] == item['i_item_sk'])
    .join(date_dim, web_sales['ws_sold_date_sk'] == date_dim['d_date_sk'])
    .join(subquery_item_id, item['i_item_id'] == subquery_item_id['i_item_id'])  # Inner join with the subquery
    .filter(
        (customer_address['ca_zip'].substr(0, 5).isin(['85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792'])) |
        (date_dim['d_qoy'] == 2),
        date_dim['d_year'] == 2000
    )
    .group_by([customer_address['ca_zip'], customer_address['ca_county']])
    .aggregate(
        total_sales=web_sales['ws_sales_price'].sum()
    )
    .order_by([ibis.asc('ca_zip'), ibis.asc('ca_county')])
    .limit(100)
)

# Step 5: Execute the query and get the result as a Pandas DataFrame
df = query.execute()

# Step 6: Print the result
print(df)

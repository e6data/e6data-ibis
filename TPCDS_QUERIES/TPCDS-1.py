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

# Step 2: Define tables
store_returns = con.table('store_returns',('glue','tpcds_1000_delta'))
date_dim = con.table('date_dim',('glue','tpcds_1000_delta'))
store = con.table('store',('glue','tpcds_1000_delta'))
customer = con.table('customer',('glue','tpcds_1000_delta'))

# Step 3: Create CTE for customer_total_return with only necessary columns
customer_total_return = (
    store_returns
    .join(date_dim, store_returns['sr_returned_date_sk'] == date_dim['d_date_sk'])
    .filter(date_dim['d_year'] == 1999)
    .group_by([store_returns['sr_customer_sk'], store_returns['sr_store_sk']])
    .aggregate(
        ctr_total_return=store_returns['sr_fee'].sum()
    )
    .select(
        store_returns['sr_customer_sk'].name('ctr_customer_sk'),
        store_returns['sr_store_sk'].name('ctr_store_sk'),
        'ctr_total_return'
    )
)

# Step 4: Join customer_total_return CTE with store and customer, selecting only necessary columns
result = (
    customer_total_return
    .join(store, customer_total_return['ctr_store_sk'] == store['s_store_sk'])
    .join(customer, customer_total_return['ctr_customer_sk'] == customer['c_customer_sk'])
    .filter(store['s_state'] == 'TN')
    .select(
        customer['c_customer_id']  # Select only the necessary column from customer
    )
    .order_by(customer['c_customer_id'])
    .limit(100)
)

# Step 5: Execute the query and get the result as a Pandas DataFrame
df = result.execute()

# Step 6: Print the result
print(df)

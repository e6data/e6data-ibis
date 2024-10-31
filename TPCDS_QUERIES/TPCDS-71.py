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
store = con.table('store', ('glue', 'tpcds_1000_delta'))
household_demographics = con.table('household_demographics', ('glue', 'tpcds_1000_delta'))
customer = con.table('customer', ('glue', 'tpcds_1000_delta'))

# Step 3: Define the subquery with all the necessary filters, calculations, and joins
subquery = (
    store_sales
    .join(date_dim, store_sales['ss_sold_date_sk'] == date_dim['d_date_sk'])
    .join(store, store_sales['ss_store_sk'] == store['s_store_sk'])
    .join(household_demographics, store_sales['ss_hdemo_sk'] == household_demographics['hd_demo_sk'])
    .filter(
        date_dim['d_dom'].between(1, 2),
        (household_demographics['hd_buy_potential'] == '>10000') | (household_demographics['hd_buy_potential'] == 'Unknown'),
        household_demographics['hd_vehicle_count'] > 0,
        (household_demographics['hd_dep_count'] / household_demographics['hd_vehicle_count']) > 1,
        date_dim['d_year'].isin([2000, 2001, 2002]),
        store['s_county'].isin(['Mobile County', 'Maverick County', 'Huron County', 'Kittitas County'])
    )
    .group_by([store_sales['ss_ticket_number'], store_sales['ss_customer_sk']])
    .aggregate(
        cnt=store_sales['ss_ticket_number'].count()
    )
    .filter(
        lambda t: t['cnt'].between(1, 5)
    )
)

# Step 4: Join the subquery with the customer table
final_query = (
    subquery
    .join(customer, subquery['ss_customer_sk'] == customer['c_customer_sk'])
    .select(
        customer['c_last_name'],
        customer['c_first_name'],
        customer['c_salutation'],
        customer['c_preferred_cust_flag'],
        subquery['ss_ticket_number'],
        subquery['cnt']
    )
    .order_by([ibis.desc(subquery['cnt']), ibis.asc(customer['c_last_name'])])
)

# Step 5: Execute the query and get the result as a Pandas DataFrame
df = final_query.execute()

# Step 6: Print the result
print(df)
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
catalog_sales = con.table('catalog_sales', ('glue', 'tpcds_1000_delta'))
date_dim = con.table('date_dim', ('glue', 'tpcds_1000_delta'))
customer_address = con.table('customer_address', ('glue', 'tpcds_1000_delta'))
call_center = con.table('call_center', ('glue', 'tpcds_1000_delta'))
catalog_returns = con.table('catalog_returns', ('glue', 'tpcds_1000_delta'))

# Step 3: Define `cs1` and `cs2` for the main query and subquery, respectively
cs1 = catalog_sales  # Main reference for `catalog_sales`
cs2 = catalog_sales.view()  # View for the subquery

# Step 4: Define the main query using Ibis expressions
query = (
    cs1
    .join(date_dim, cs1['cs_ship_date_sk'] == date_dim['d_date_sk'])
    .join(customer_address, cs1['cs_ship_addr_sk'] == customer_address['ca_address_sk'])
    .join(call_center, cs1['cs_call_center_sk'] == call_center['cc_call_center_sk'])
    .filter(
        (date_dim['d_date'].between('2001-04-01', ibis.literal('2001-04-01').cast('date') + ibis.interval(days=60))),
        customer_address['ca_state'] == 'NY',
        call_center['cc_county'].isin(['Ziebach County', 'Levy County', 'Huron County', 'Franklin Parish', 'Daviess County'])
    )
    # EXISTS condition to check for same order number but different warehouse
    .filter(
        cs1['cs_order_number'].isin(
            cs2.filter(
                cs2['cs_order_number'] == cs1['cs_order_number'],
                cs2['cs_warehouse_sk'] != cs1['cs_warehouse_sk']
            )['cs_order_number']
        )
    )
    # NOT EXISTS condition to exclude orders in catalog_returns
    .filter(
        ~cs1['cs_order_number'].isin(catalog_returns['cr_order_number'])
    )
    .aggregate(
        order_count=cs1['cs_order_number'].nunique(),  # Count distinct order numbers
        total_shipping_cost=cs1['cs_ext_ship_cost'].sum(),
        total_net_profit=cs1['cs_net_profit'].sum()
    )
    .order_by(ibis.desc('order_count'))
    .limit(100)
)

# Step 5: Execute the query and get the result as a Pandas DataFrame
df = query.execute()

# Step 6: Print the result
print(df)

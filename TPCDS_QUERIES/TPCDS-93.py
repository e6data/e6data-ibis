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
store_returns = con.table('store_returns', ('glue', 'tpcds_1000_delta'))
reason = con.table('reason', ('glue', 'tpcds_1000_delta'))

# Step 3: Define the subquery with the `CASE` statement to compute `act_sales`
subquery = (
    store_sales
    .left_join(store_returns, [
        store_sales['ss_item_sk'] == store_returns['sr_item_sk'],
        store_sales['ss_ticket_number'] == store_returns['sr_ticket_number']
    ])
    .join(reason, store_returns['sr_reason_sk'] == reason['r_reason_sk'])
    .filter(reason['r_reason_desc'] == 'Did not like the warranty')
    .mutate(
        act_sales=ibis.case()
            .when(store_returns['sr_return_quantity'].isnull(), store_sales['ss_quantity'] * store_sales['ss_sales_price'])
            .else_((store_sales['ss_quantity'] - store_returns['sr_return_quantity']) * store_sales['ss_sales_price'])
            .end()
    )
)

# Step 4: Aggregate the results by `ss_customer_sk` and sum `act_sales`
query = (
    subquery
    .group_by(subquery['ss_customer_sk'])
    .aggregate(
        sumsales=subquery['act_sales'].sum()
    )
    .order_by([ibis.asc('sumsales'), subquery['ss_customer_sk']])
    .limit(100)
)

# Step 5: Execute the query and get the result as a Pandas DataFrame
df = query.execute()

# Step 6: Print the result
print(df)
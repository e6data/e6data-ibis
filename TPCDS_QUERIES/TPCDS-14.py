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
inventory = con.table('inventory', ('glue', 'tpcds_1000_delta'))
warehouse = con.table('warehouse', ('glue', 'tpcds_1000_delta'))
item = con.table('item', ('glue', 'tpcds_1000_delta'))
date_dim = con.table('date_dim', ('glue', 'tpcds_1000_delta'))

# Define key date for comparison
comparison_date = ibis.literal('1998-04-08').cast('date')

# Step 3: Define the main query using Ibis expressions
query = (
    inventory
    .join(warehouse, inventory['inv_warehouse_sk'] == warehouse['W_WAREHOUSE_SK'])
    .join(item, inventory['inv_item_sk'] == item['i_item_sk'])
    .join(date_dim, inventory['inv_date_sk'] == date_dim['d_date_sk'])
    .filter(
        item['i_current_price'].between(0.99, 1.49),
        date_dim['d_date'].between(
            comparison_date - ibis.interval(days=30),
            comparison_date + ibis.interval(days=30)
        )
    )
    .group_by([warehouse['W_WAREHOUSE_NAME'], item['i_item_id']])
    .aggregate(
        inv_before=ibis.case()
            .when(date_dim['d_date'] < comparison_date, inventory['inv_quantity_on_hand'])
            .else_(0)
            .end().sum(),
        inv_after=ibis.case()
            .when(date_dim['d_date'] >= comparison_date, inventory['inv_quantity_on_hand'])
            .else_(0)
            .end().sum(),
    )
)

# Step 4: Mutate and add inv_ratio to the query
query = query.mutate(
    inv_ratio=ibis.case()
        .when(query.inv_before > 0, query.inv_after / query.inv_before)
        .else_(None)
        .end()
)

# Step 5: Apply filter on inv_ratio
query = query.filter(
    (ibis.literal(2.0 / 3.0) <= query.inv_ratio) & (query.inv_ratio <= ibis.literal(3.0 / 2.0))
)

# Step 6: Order and limit the result
query = query.order_by([warehouse['W_WAREHOUSE_NAME'], item['i_item_id']]).limit(100)

# Step 7: Execute the query and get the result as a Pandas DataFrame
df = query.execute()

# Step 8: Print the result
print(df)

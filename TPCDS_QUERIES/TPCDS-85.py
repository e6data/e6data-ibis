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
date_dim = con.table('date_dim', ('glue', 'tpcds_1000_delta'))
item = con.table('item', ('glue', 'tpcds_1000_delta'))

# Step 3: Emulating GROUPING function in Ibis
category_is_grouped = item['i_category'].isnull().cast('int8')
class_is_grouped = item['i_class'].isnull().cast('int8')
lochierarchy = category_is_grouped + class_is_grouped

# Step 4: Group and Aggregate First
aggregated_query = (
    web_sales
    .join(date_dim, web_sales['ws_sold_date_sk'] == date_dim['d_date_sk'])
    .join(item, web_sales['ws_item_sk'] == item['i_item_sk'])
    .filter(date_dim['d_month_seq'].between(1212, 1212 + 11))
    .group_by([item['i_category'], item['i_class'], lochierarchy])
    .aggregate(
        total_sum=web_sales['ws_net_paid'].sum()
    )
    .mutate(lochierarchy=lochierarchy)  # Ensure we carry lochierarchy after aggregation
)

# Step 5: Define the rank window without using ROWS
rank_window = ibis.window(
    group_by=[aggregated_query['lochierarchy'], aggregated_query['i_category']],
    order_by=aggregated_query['total_sum'].desc()
).group_by([aggregated_query['lochierarchy'], aggregated_query['i_category']])

# Step 6: Apply the window function for ranking after aggregation
final_query = (
    aggregated_query
    .mutate(
        rank_within_parent=ibis.rank().over(rank_window)
    )
    .order_by([
        ibis.desc('lochierarchy'),
        ibis.case()
            .when(aggregated_query['lochierarchy'] == 0, aggregated_query['i_category'])
            .end(),
        'rank_within_parent'
    ])
    .limit(100)
)

# Step 7: Execute the query and get the result as a Pandas DataFrame
df = final_query.execute()

# Step 8: Print the result
print(df)
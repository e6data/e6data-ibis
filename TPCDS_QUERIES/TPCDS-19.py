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
customer_demographics = con.table('customer_demographics', ('glue', 'tpcds_1000_delta'))
date_dim = con.table('date_dim', ('glue', 'tpcds_1000_delta'))
item = con.table('item', ('glue', 'tpcds_1000_delta'))
promotion = con.table('promotion', ('glue', 'tpcds_1000_delta'))

# Step 3: Define the main query using Ibis expressions
query = (
    catalog_sales
    .join(date_dim, catalog_sales['cs_sold_date_sk'] == date_dim['d_date_sk'])
    .join(item, catalog_sales['cs_item_sk'] == item['i_item_sk'])
    .join(customer_demographics, catalog_sales['cs_bill_cdemo_sk'] == customer_demographics['cd_demo_sk'])
    .join(promotion, catalog_sales['cs_promo_sk'] == promotion['p_promo_sk'])
    .filter(
        customer_demographics['cd_gender'] == 'F',
        customer_demographics['cd_marital_status'] == 'W',
        customer_demographics['cd_education_status'] == 'Primary',
        (promotion['p_channel_email'] == 'N') | (promotion['p_channel_event'] == 'N'),
        date_dim['d_year'] == 1998
    )
    .group_by(item['i_item_id'])
    .aggregate(
        agg1=catalog_sales['cs_quantity'].mean(),
        agg2=catalog_sales['cs_list_price'].mean(),
        agg3=catalog_sales['cs_coupon_amt'].mean(),
        agg4=catalog_sales['cs_sales_price'].mean()
    )
    .order_by(item['i_item_id'])
    .limit(100)
)

# Step 4: Execute the query and get the result as a Pandas DataFrame
df = query.execute()

# Step 5: Print the result
print(df)

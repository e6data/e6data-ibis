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

catalog_sales = con.table('catalog_sales',('glue','tpcds_1000_delta'))
customer = con.table('customer',('glue','tpcds_1000_delta'))
customer_address = con.table('customer_address',('glue','tpcds_1000_delta'))
date_dim = con.table('date_dim',('glue','tpcds_1000_delta'))


# 1. datasets - delta format - TPCDS_1000_DELTA
# 2. Bunch of sql E6 queries - unknown to world/ibis
# 3. Ibis by default assumes duckDB dialect if none is provided and one runs on duckdb

query = (
    catalog_sales
    .join(customer, catalog_sales['cs_bill_customer_sk'] == customer['c_customer_sk'])
    .join(customer_address, customer['c_current_addr_sk'] == customer_address['ca_address_sk'])
    .join(date_dim, catalog_sales['cs_sold_date_sk'] == date_dim['d_date_sk'])
    .filter(
        (customer_address['ca_zip'].substr(0, 5).isin(['85669', '86197', '88274',
                                                       '83405', '86475', '85392', '85460', '80348', '81792'])) |
        (customer_address['ca_state'].isin(['CA', 'WA', 'GA'])) |
        (catalog_sales['cs_sales_price'] > 500),
        date_dim['d_qoy'] == 2,
        date_dim['d_year'] == 2000
    )
    .group_by(customer_address['ca_zip'])
    .aggregate(
        total_sales=catalog_sales['cs_sales_price'].sum()
    )
    .order_by(customer_address['ca_zip'])
    .limit(100)
)
df = query.execute()
ibis_query = ibis.to_sql(df)
print(f"ibis query:\n{ibis_query}\n")
print(df)

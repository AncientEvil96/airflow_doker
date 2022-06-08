from base.my import MySQL
from sys import argv
from base.bases_metods import convert_dict_args
from pathlib import Path

sours_params_s = argv[1:]
local_dir = f'/tmp/tmp/'


def file_sensor():
    supermarket_path = Path(local_dir)
    data_files = supermarket_path.glob("amount_maria.parquet.gzip")
    if len(list(data_files)) > 0:
        exit(0)
    else:
        print('no file')
        exit(1)


def extract_change_amount(sourse):
    query = f"""
            SELECT product_priority_stock.product_id                            as product_id,
                   s.name                                                       as stock_name,
                   product_priority_stock.amount + ifnull(new_orders.amount, 0) as product_amount
            FROM product_priority_stock
                     JOIN stock s on product_priority_stock.stock_id = s.id
                     LEFT JOIN (select order_product.product_id  as product_id,
                                       1_2_status.stock_id       as stock_id,
                                       SUM(order_product.amount) as amount
                                FROM order_product
                                         JOIN (select order.id as id, stock_id as stock_id
                                               FROM `order`
                                               WHERE status in (1, 2)
                                                 and is_main = 0) as 1_2_status
                                              ON order_id = 1_2_status.id
                                GROUP BY order_product.product_id, 1_2_status.stock_id) as new_orders
                               ON product_priority_stock.product_id = new_orders.product_id
                                   and s.id = new_orders.stock_id
            WHERE product_priority_stock.amount - ifnull(new_orders.amount, 0) > 0
;
            """
    sourse.connection_init()
    df = sourse.select_to_df(query)
    sourse.connection_close()
    df.to_parquet(f'{local_dir}amount_maria.parquet.gzip', compression='gzip')


if __name__ == '__main__':
    sours_params = convert_dict_args(sours_params_s)

    sourse = MySQL(
        params=sours_params
    )
    extract_change_amount(sourse)
    file_sensor()

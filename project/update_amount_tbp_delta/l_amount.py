from base.my import MySQL
from sys import argv
from base.operations_to_files import File
from base.bases_metods import convert_dict_args

sours_params_s, path = argv[1:]


def my_sql_select(source):

    query = f"""
                drop table if exists tmp_amount;
                CREATE TEMPORARY TABLE tmp_amount
                (
                    operacion  char(1),
                    product_id integer,
                    stock_name nvarchar(50),
                    amount     integer
                );
                
                create unique index tmp_amount_uq_in
                    on tmp_amount (product_id, stock_name);
                
                LOAD DATA INFILE '/tmp/load/{path}/amount.csv'
                    INTO TABLE tmp_amount
                    fields terminated by '\t'
                    LINES TERMINATED BY '\n'
                ;
                
                CREATE or replace TEMPORARY TABLE tmp_update_data as
                SELECT product_id, stock_id
                FROM (SELECT tmp_amount.product_id as product_id,
                             tmp_amount.amount     as amount,
                             stock.id              as stock_id
                      FROM tmp_amount
                               INNER JOIN product on tmp_amount.product_id = product.id
                               INNER JOIN stock ON stock.name = tmp_amount.stock_name) as new_data
                         INNER JOIN product_priority_stock USING (product_id, stock_id)
                WHERE new_data.amount = 0;
                SET autocommit = 0;
                START TRANSACTION;
                DELETE
                    pps
                FROM product_priority_stock as pps
                         INNER JOIN tmp_update_data as b
                                    USING (product_id, stock_id)
                WHERE pps.product_id = b.product_id
                  and pps.stock_id = b.stock_id;
                CREATE or replace TEMPORARY TABLE tmp_update_data as
                SELECT product_id,
                       stock_id,
                       new_data_.amount - ifnull(new_orders.amount, 0) as amount
                FROM (SELECT tmp_amount.product_id as product_id,
                             tmp_amount.amount     as amount,
                             stock.id              as stock_id
                      FROM tmp_amount
                               INNER JOIN product on tmp_amount.product_id = product.id
                               INNER JOIN stock ON stock.name = tmp_amount.stock_name) as new_data_
                         INNER JOIN product_priority_stock USING (product_id, stock_id)
                         LEFT JOIN (select order_product.product_id  as product_id,
                                           1_2_status.stock_id       as stock_id,
                                           SUM(order_product.amount) as amount
                                    FROM order_product
                                             JOIN (select id, stock_id
                                                   FROM `order`
                                                   WHERE status in (1, 2)
                                                     and is_main = 0) as 1_2_status
                                                  ON order_id = 1_2_status.id
                                    GROUP BY order_product.product_id, 1_2_status.stock_id) as new_orders 
                                        USING (product_id, stock_id)
                ;
                
                UPDATE product_priority_stock
                    INNER JOIN tmp_update_data as new_data
                    ON new_data.product_id = product_priority_stock.product_id
                        AND new_data.stock_id = product_priority_stock.stock_id
                SET product_priority_stock.amount = new_data.amount
                WHERE new_data.amount <> product_priority_stock.amount;
                
                CREATE or replace TEMPORARY TABLE tmp_update_data as
                SELECT tmp_amount.product_id                            as product_id,
                       tmp_amount.amount - ifnull(new_orders.amount, 0) as amount,
                       stock.id                                         as stock_id,
                       stock.priority_id                                as priority_id
                FROM tmp_amount
                         INNER JOIN product on tmp_amount.product_id = product.id
                         INNER JOIN stock ON stock.name = tmp_amount.stock_name
                         LEFT JOIN (select order_product.product_id  as product_id,
                                           1_2_status.stock_id       as stock_id,
                                           SUM(order_product.amount) as amount
                                    FROM order_product
                                             JOIN (select id, stock_id
                                                   FROM `order`
                                                   WHERE status in (1, 2)
                                                     and is_main = 0) as 1_2_status
                                                  ON order_id = 1_2_status.id
                                    GROUP BY order_product.product_id, 1_2_status.stock_id) as new_orders
                                   ON product.id = new_orders.product_id
                                       and stock.id = new_orders.stock_id
                WHERE tmp_amount.amount - ifnull(new_orders.amount, 0) > 0;
                
                INSERT INTO product_priority_stock (product_id,
                                                    stock_id,
                                                    priority_id,
                                                    amount,
                                                    created_at,
                                                    updated_at)
                SELECT new_data_.product_id  as product_id,
                       new_data_.stock_id    as stock_id,
                       new_data_.priority_id as priority_id,
                       new_data_.amount      as amount,
                       UNIX_TIMESTAMP(now()) as created_at,
                       UNIX_TIMESTAMP(now()) as updated_at
                FROM tmp_update_data as new_data_
                         LEFT JOIN product_priority_stock
                                   ON product_priority_stock.product_id = new_data_.product_id
                                       AND product_priority_stock.stock_id = new_data_.stock_id
                WHERE product_priority_stock.id is null;
                COMMIT;
                SET autocommit = 1;
                
                """.split(';')

    source.connection_init()
    for q in query:
        q = q.strip()
        if q != '':
            source.query_to_base(q)
    source.connection_close()


if __name__ == '__main__':
    sours_params = convert_dict_args(sours_params_s)

    my_sql = MySQL(
        params=sours_params
    )
    my_sql_select(my_sql)

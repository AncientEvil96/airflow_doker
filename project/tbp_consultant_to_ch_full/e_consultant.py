from base.my import MySQL
from sys import argv
from datetime import datetime, timedelta
from base.operations_to_files import File
from base.bases_metods import convert_dict_args
from base.bases_metods import get_date

begin_dt, sours_params_s = argv[1:]
local_dir = f'/tmp/tmp/{begin_dt}/'


def my_sql_select(source, query):
    source.connection_init()
    df = source.select_to_df(query)
    source.connection_close()
    return df


if __name__ == '__main__':
    date_now = datetime.now()
    t_begin = get_date(begin_dt)
    t_end = t_begin.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

    sours_params = convert_dict_args(sours_params_s)

    my_sql = MySQL(
        params=sours_params
    )

    query = f"""
    SELECT IFNULL(consultant_message.id, '')                                             as id,
           IFNULL(consultant_message.consultant_user_id, '')                             as consultant_user_id,
           IFNULL(consultant_message.user_id, '')                                        as user_id,
           IFNULL(consultant_message.user_type, '')                                      as user_type,
           IFNULL(REPLACE(REPLACE(REPLACE(consultant_message.message, '\t', ' '), '\n', ' '), '  ', ' '),
                  '')                                                                    as message,
           FROM_UNIXTIME(consultant_message.created_at)                                  as created_at,
           FROM_UNIXTIME(consultant_message.updated_at)                                  as updated_at,
           IFNULL(consultant_message.image, '')                                          as image,
           IFNULL(consultant_message.viewed, '')                                         as viewed,
           IFNULL(consultant_message.notification_id, '')                                as notification_id,
           IFNULL(consultant_message.complaint, '')                                      as complaint,
           IFNULL(consultant_message.type, '')                                           as type,
           IFNULL(consultant_profile.city_id, '')                                        as city_id,
           IFNULL(consultant_profile.name, '')                                           as consultant_user_name,
           IFNULL(FROM_UNIXTIME(consultant_profile.birth_date), '1970-01-01 00:00:00') as birth_date,
           IFNULL(consultant_profile.gender, '')                                         as gender,
           IFNULL(user_profile.name, '')                                                 as user_name,
           IFNULL(notification.title, '')                                                as notification_title,
           IFNULL(notification.name, '')                                                 as notification_name,
           IFNULL(REPLACE(REPLACE(REPLACE(notification.body, '\t', ' '), '\n', ' '), '  ', ' '),
                  '')                                                                    as notification_body,
           IFNULL(notification.model_type, '')                                           as notification_model_type,
           IFNULL(gc.geo_region_id, '')                                                  as gc_geo_region_id,
           IFNULL(gc.name, '')                                                           as gc_name,
           IFNULL(gc.longitude, '')                                                      as gc_longitude,
           IFNULL(gc.latitude, '')                                                       as gc_latitude,
           IFNULL(gr.name, '')                                                           as gr_name
    FROM consultant_message
             LEFT JOIN user_profile as consultant_profile
                       ON consultant_message.consultant_user_id = consultant_profile.user_id
             LEFT JOIN user_profile
                       ON consultant_message.consultant_user_id = user_profile.user_id
             LEFT JOIN notification
                       ON consultant_message.notification_id = notification.id
             LEFT JOIN geo_city gc on consultant_profile.city_id = gc.id
             LEFT JOIN geo_region gr on gc.geo_region_id = gr.id
    Where {f"FROM_UNIXTIME(consultant_message.updated_at) < '{t_end}'" if begin_dt == '20200101' else f"FROM_UNIXTIME(consultant_message.updated_at) between '{t_begin}' and '{t_end}'"}
    ;
    """

    df = my_sql_select(my_sql, query)

    if len(df) == 0:
        print('no data')
        exit(1)

    file = File(f'{local_dir}consultant')
    file.create_file_parquet(df)

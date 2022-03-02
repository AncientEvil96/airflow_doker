from base.my import MySQL
from sys import argv

sours_params_s = argv[1]
local_dir = '/tmp/tmp/'

table = 'tmp_sub_class'


def translit_text(text):
    dictionary = {
        'а': 'a',
        'б': 'b',
        'в': 'v',
        'г': 'g',
        'д': 'd',
        'е': 'e',
        'ё': 'e',
        'ж': 'zh',
        'з': 'z',
        'и': 'i',
        'й': 'i',
        'к': 'k',
        'л': 'l',
        'м': 'm',
        'н': 'n',
        'о': 'o',
        'п': 'p',
        'р': 'r',
        'с': 's',
        'т': 't',
        'у': 'u',
        'ф': 'f',
        'х': 'h',
        'ц': 'c',
        'ч': 'cz',
        'ш': 'sh',
        'щ': 'scz',
        'ъ': '',
        'ы': 'y',
        'ь': 'b',
        'э': 'e',
        'ю': 'u',
        'я': 'ja',
        'А': 'A',
        'Б': 'B',
        'В': 'V',
        'Г': 'G',
        'Д': 'D',
        'Е': 'E',
        'Ё': 'E',
        'Ж': 'ZH',
        'З': 'Z',
        'И': 'I',
        'Й': 'I',
        'К': 'K',
        'Л': 'L',
        'М': 'M',
        'Н': 'N',
        'О': 'O',
        'П': 'P',
        'Р': 'R',
        'С': 'S',
        'Т': 'T',
        'У': 'U',
        'Ф': 'F',
        'Х': 'H',
        'Ц': 'C',
        'Ч': 'CZ',
        'Ш': 'SH',
        'Щ': 'SCH',
        'Ъ': '',
        'Ы': 'y',
        'Ь': 'b',
        'Э': 'E',
        'Ю': 'U',
        'Я': 'YA',
        '№': '#',
        'ґ': 'r',
        'ї': 'r',
        'є': 'e',
        'Ґ': 'g',
        'Ї': 'i',
        'Є': 'e',
        '—': '-',
        ',': '',
        ' ': '-'
    }
    s = ''
    for i in text:
        s = s + dictionary.get(i, i)

    return s


if __name__ == '__main__':
    s = str(sours_params_s).replace('[', '').replace(']', '').replace("'", '').replace('(', '').replace(')', '').split(
        ',')
    sours_params = dict(zip(s[::2], s[1::2]))

    target = MySQL(
        params=sours_params
    )

    target.connection_init()

    df = target.select_to_df(
        """
            SELECT Subdivision.Subdivision_ID                             as Subdivision_ID,
                   Subdivision.Catalogue_ID                               as Catalogue_ID,
                   IF(have_children.Subdivision_ID is not null, 175, 176) as Class_ID,
                   Subdivision.Subdivision_Name                           as Sub_Class_Name
            FROM Subdivision
                     LEFT JOIN Sub_Class
                               ON Sub_Class.Subdivision_ID = Subdivision.Subdivision_ID
                                   AND Sub_Class.Catalogue_ID = Subdivision.Catalogue_ID
                                   AND Sub_Class.Catalogue_ID in (1, 2)
                     LEFT JOIN (SELECT Subdivision.Subdivision_ID
                                FROM Subdivision
                                         INNER JOIN Subdivision as Children
                                                    ON Subdivision.Subdivision_ID = Children.Parent_Sub_ID
                                GROUP BY Subdivision.Subdivision_ID) as have_children
                               ON Subdivision.Subdivision_ID = have_children.Subdivision_ID
            WHERE Subdivision.Catalogue_ID in (1, 2)
              AND Sub_Class.Sub_Class_ID is null
            ;
        """
    )

    df['EnglishName'] = list(map(translit_text, df['Sub_Class_Name']))

    target.query_to_base(f'drop table if exists {table};')
    target.query_to_base(
        f"""
            create temporary table {table}
            (
                Subdivision_ID int                 not null,
                Catalogue_ID   int                 not null,
                Class_ID       int                 not null,
                Sub_Class_Name varchar(255)        not null,
                Checked        smallint default 1  not null,
                CustomSettings text     default '' not null,
                EnglishName    varchar(64)         not null
            );
        """
    )

    query = f"""
                INSERT INTO {table}
                    (
                        Subdivision_ID,
                        Catalogue_ID,
                        Class_ID,
                        Sub_Class_Name,
                        EnglishName
                     )
                VALUES (%s, %s, %s, %s, %s);
            """

    target.load_many_to_base(query, list(df.itertuples(index=False, name=None)))

    target.query_to_base(
        f"""
            INSERT INTO Sub_Class
                (Subdivision_ID,
                 Catalogue_ID,
                 Class_ID,
                 Sub_Class_Name,
                 Checked,
                 CustomSettings,
                 EnglishName)
                 select * from {table};
        """
    )

    target.connection_close()

from transliterate import translit
from base.my import MySQL
from sys import argv

host, port, password, login, database, file = argv[1:]


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
        ',': ',',
        '?': '?',
        ' ': '-',
        '~': '~',
        '!': '!',
        '@': '@',
        '#': '#',
        '$': '$',
        '%': '%',
        '^': '^',
        '&': '&',
        '*': '*',
        '(': '(',
        ')': ')',
        '-': '-',
        '=': '=',
        '+': '+',
        ':': ':',
        ';': ';',
        '<': '<',
        '>': '>',
        '\'': '\'',
        '"': '"',
        '\\': '\\',
        '/': '/',
        '№': '#',
        '[': '[',
        ']': ']',
        '{': '{',
        '}': '}',
        'ґ': 'r',
        'ї': 'r',
        'є': 'e',
        'Ґ': 'g',
        'Ї': 'i',
        'Є': 'e',
        '—': '-',
        '0': '0',
        '1': '1',
        '2': '2',
        '3': '3',
        '4': '4',
        '5': '5',
        '6': '6',
        '7': '7',
        '8': '8',
        '9': '9'
    }
    s = ''
    for i in text:
        s = s + dictionary.get(i)

    return s


if __name__ == '__main__':
    target = MySQL(
        params={
            'host': host,
            'port': port,
            'password': password,
            'login': login,
            'database': database,
        }
    )

    target.connection_init()

    df = target.select_to_df(
        """
            SELECT Subdivision.Subdivision_ID                             as Subdivision_ID,
                   Subdivision.Catalogue_ID                               as Catalogue_ID,
                   IF(have_children.Subdivision_ID is not null, 175, 176) as Class_ID,
                   Subdivision.Subdivision_Name                           as Sub_Class_Name,
                   1                                                      as Checked
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

    df['CustomSettings'] = ''
    df['EnglishName'] = list(map(translit_text, df['Sub_Class_Name']))
    target.connection_close()

    print(df)

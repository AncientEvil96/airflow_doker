from base.my import MySQL
from sys import argv

sours_params_s = argv[1]
# sours_params_s = "[('host','84.38.187.211'),('port',32106),('password','Zrn5qDfXGklpJ59'),('login','vprok_transfer'),('database','vprok')]"
local_dir = '/tmp/tmp/'
# local_dir = ''

table = 'tmp_checked'

if __name__ == '__main__':
    s = str(sours_params_s).replace('[', '').replace(']', '').replace("'", '').replace('(', '').replace(')', '').split(
        ',')
    sours_params = dict(zip(s[::2], s[1::2]))

    target = MySQL(
        params=sours_params
    )

    target.connection_init()

    target.query_to_base(f'drop table if exists {table};')
    target.query_to_base(
        """
        CREATE TABLE tt1
            (Subdivision_ID int)
        WITH RECURSIVE
            rec (Subdivision_ID)
        AS
        (
          SELECT Subdivision_ID as Subdivision_ID FROM Message173 GROUP BY Subdivision_ID
          UNION
          SELECT Parent_Sub_ID as Subdivision_ID
          FROM Subdivision as S
              INNER JOIN rec ON rec.Subdivision_ID = S.Subdivision_ID
        )
        SELECT * FROM rec;
        """
    )

    target.query_to_base(
        """
        insert into tt1
        WITH RECURSIVE
            rec (Subdivision_ID)
        AS
        (
          SELECT Subdivision_ID as Subdivision_ID FROM Message176 GROUP BY Subdivision_ID
          UNION
          SELECT Parent_Sub_ID as Subdivision_ID
          FROM Subdivision as S
              INNER JOIN rec ON rec.Subdivision_ID = S.Subdivision_ID
        )
        SELECT * FROM rec;
        """
    )

    target.query_to_base(
        """
        UPDATE Subdivision
        SET Subdivision.Checked = 1
        WHERE Subdivision_ID in (SELECT Subdivision_ID FROM tt1 GROUP BY Subdivision_ID);
        """
    )

    target.query_to_base(
        """
        UPDATE Subdivision
        SET Subdivision.Checked = 0
        WHERE Subdivision_ID not in (SELECT Subdivision_ID FROM tt1 GROUP BY Subdivision_ID);
        """
    )

    target.connection_close()

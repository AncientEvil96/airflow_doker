import os


class File:
    def __init__(self, file_name):
        self.file_name = file_name

    def create_file_parquet(self, df) -> str or None:
        try:
            if len(df) == 0:
                return None

            file_name = f'{self.file_name}.parquet.gzip'
            df.to_parquet(file_name, compression='gzip')
            print(f'Created {file_name}')
            return file_name
        except Exception as err:
            print(err, ':', self.file_name)
            return None

    def read_file_parquet(self, df) -> str or None:
        try:
            if len(df) == 0:
                return None

            file_name = f'{self.file_name}.parquet.gzip'
            df.to_parquet(file_name, compression='gzip')
            print(f'Created {file_name}')
            return file_name
        except Exception as err:
            print(err, ':', self.file_name)
            return None

    def create_file_csv(self, df) -> str or None:
        try:
            if len(df) == 0:
                return None

            file_name = f'{self.file_name}.csv'
            df.to_cvs(file_name,  sep='\t')
            print(f'Created {file_name}')
            return file_name
        except Exception as err:
            print(err, ':', self.file_name)
            return None

    def delete_file(self):
        os.remove(self.file_name)

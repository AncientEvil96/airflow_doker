class SaveFile:

    def create_file_parquet(self, df, file_name) -> str or None:
        try:
            if len(df) == 0:
                return None

            file_name = f'{file_name}.parquet.gzip'
            df.to_parquet(file_name, compression='gzip')
            print(f'Created {file_name}')
            return file_name
        except Exception as err:
            print(err, ':', file_name)
            return None

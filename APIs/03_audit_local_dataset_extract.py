import tableaudocumentapi as DoC
import json
import datetime as dt
from pathlib import Path
from glob import glob

now = dt.datetime.today().strftime('%Y%m%d%H%M%S')
print(now)

def audit_local_datasets(local_folder):
    under_review = ['customer', '2018-19 Arsenal Player Stats']
    download_path = local_folder
    datasource_extension = ''

    for ds_file in glob(download_path + '*'):
        if Path(ds_file).suffix in ['.tdsx', '.hyper']:
            datasource_extension = Path(ds_file).suffix

    for i, r in enumerate(under_review):
        review = under_review[i]
        reviewed = DoC.Datasource.from_file(download_path + review + datasource_extension)
        new_name = DoC.Datasource.name
        new_caption = DoC.Datasource.caption

        print('-'*50)
        print(f'{"-"*3}{len(reviewed.fields)} total fields in {review} datasource')
        print('-'*50)

        audit_file = download_path + str(now) + '_' + review + '.json'

        with open(audit_file, 'a') as bm:   # audit backend fields and metadata
            data_fields = []
            for count, field in enumerate(reviewed.fields.values()):
                df = {
                    "field_id": field.id,
                    "field_name": field.name,
                    "field_datatype": field.datatype,
                    "field_default_aggregationn": field.default_aggregation,
                    "field_calculation": field.calculation,
                    "field_description": field.description
                }

                data_fields.append(df)
            json.dump(data_fields, bm, indent=2)

        print(f'Check {audit_file} for further details...:\n')

if __name__ == '__main__':
    audit_local_datasets('C:/Users/angelinat/Desktop/')

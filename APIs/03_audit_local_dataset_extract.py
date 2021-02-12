import tableaudocumentapi as DoC
import json
import datetime as dt
from pathlib import Path
from glob import glob

now = dt.datetime.today().strftime('%Y%m%d%H%M%S')
print(now)

def audit_local_datasets(local_folder, *args):
    # pass the directory and the datasource names to audit
    under_review = []
    for a in args:
        under_review.append(a)

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
        calucalted_fields_file = download_path + str(now) + '_' + review + '_calculated.json'

        # get all fields
        with open(audit_file, 'a') as all_fields:
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
            json.dump(data_fields, all_fields, indent=2)


        # get calucalted_fields only
        with open(calucalted_fields_file, 'a') as calcs:
            calculated_fields = []
            for count, field in enumerate(reviewed.fields.values()):
                if field.calculation:
                    cf = {
                            "field_id": field.id,
                            "field_name": field.name,
                            "field_calculation": field.calculation,
                            "field_description": field.description
                            }
                    calculated_fields.append(cf)
            json.dump(calculated_fields, calcs, indent=2)

        print(f'There were {len(calculated_fields)} calculated fields')
        print(f'Check {audit_file} and {calucalted_fields_file} for further details...:\n')

if __name__ == '__main__':
    audit_local_datasets('C:/Users/angelinat/Desktop/', 'customer', '2018-19 Arsenal Player Stats')

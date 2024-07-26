import tableauserverclient as TSC
import datetime as dt
from PyPDF2 import PdfFileMerger, PdfFileReader


def download_as_pdf(tableau_server, tableau_user, user_password, site_name, download_path, *args):
    # if you're connecting to the default site, pass empty string in site_name
    # pass workbook names after downnload path -> e.g 'Tableau Onboarding', 'VizAlertsDemo'

    now = dt.datetime.today().strftime('%Y-%m-%d %H-%M-%S')
    today = dt.datetime.today().strftime('%Y-%m-%d')
    print(today, now)

    # compile list of workbooks to download as pdf
    generate_pdf = []
    for a in args:
        generate_pdf.append(a)

    # authenticate with Tableau Server
    tableau_auth = TSC.TableauAuth(tableau_user, user_password, site_id=site_name)
    server = TSC.Server(tableau_server, use_server_version=True)

    with server.auth.sign_in(tableau_auth):
        for item in generate_pdf:
            # pagination item can only list the first 100, therefore use request options to filter for specific dataset/wbk
            req_option = TSC.RequestOptions()
            req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name, TSC.RequestOptions.Operator.Equals, item))

            published, pagination_item = server.workbooks.get(req_option)
            print("\nThere are {} workbooks on site: ".format(pagination_item.total_available))

            for wbk in published:
                server.workbooks.populate_connections(wbk)

                # get the used datasources in the wbk
                connection_info = [connection.datasource_name for connection in wbk.connections]
                print(connection_info)

                if len(connection_info) > 0:
                    print(
                        wbk.id,
                        wbk.name, '_',
                        wbk.connections[0].connection_type, '_',
                        wbk.connections[0].id, '_',
                        wbk.connections[0].username, '_',
                        wbk.connections[0].server_address
                    )

                # specify PDF landsscape
                pdf_req_option = TSC.PDFRequestOptions(page_type=TSC.PDFRequestOptions.PageType.A4,
                                                       orientation=TSC.PDFRequestOptions.Orientation.Landscape)

                # download wbk as separate page PDFs
                pdf_files = []
                server.workbooks.populate_views(wbk)

                for v in wbk.views:
                    print(v.id, ' | ', v.name)
                    pdf_files.append(v.name)
                    server.views.populate_pdf(v, pdf_req_option)
                    saves = download_path + v.name + '.pdf'

                    with open(saves, 'wb') as f:
                        f.write(v.pdf)

                print(f'{len(pdf_files)} files downloaded, {pdf_files}')

                # merge all PDFs
                mergedObject = PdfFileMerger()

                for f in pdf_files:
                    mergedObject.append(PdfFileReader(download_path + f + '.pdf', 'rb'))

                mergedObject.write(download_path + str(today) + ' ' + wbk.name + '.pdf')

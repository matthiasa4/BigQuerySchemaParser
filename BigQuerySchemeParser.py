#!/usr/bin/env python

from cement.core.controller import CementBaseController, expose
from cement.core.foundation import CementApp
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials

credentials = GoogleCredentials.get_application_default()
bigquery_service = build('bigquery', 'v2', credentials=credentials)

class BigQueryController(CementBaseController):
    class Meta:
        label = 'base'
        description = "This CLI tool downloads a BigQuery schema and parses it into the format ready to be re-used in" \
                      " for e.g. the BigQuery GUI or when writing Apache Beam pipelines using the Python SDK." \
                      "\n \n" \
                      "You have to be authenticated using \"gcloud auth application-default login\" to use this tool."
        arguments = [
            (['--project'],
             dict(action='store', help='Specify the project in which the dataset resides.')),
            (['--dataset'],
             dict(action='store', help='Specify the dataset in which the table resides.')),
            (['--table'],
             dict(action='store', help='Specify the name of the table.')),
            (['--output'],
             dict(action='store',
                  help='Specify the output file you want the schema written to. '
                       'If not specified it will be written to \'output.txt\''))
        ]

    @expose(hide=True)
    def default(self):
        if self.app.pargs.project and self.app.pargs.dataset and self.app.pargs.table:
            schema = bigquery_service.tables().get(projectId=self.app.pargs.project, datasetId=self.app.pargs.dataset,
                                                   tableId=self.app.pargs.table).execute()
            if self.app.pargs.output:
                filename = self.app.pargs.output
            else:
                filename = 'output.txt'

            with open(filename, 'w') as f:
                for e in schema["schema"]['fields']:
                    f.write(e['name'] + ':' + e['type'] + ', \n')
        else:
            self.app.log.error('One of the necessary parameters - project, dataset or output - is missing')
            print(app.args.print_help())

class MyApp(CementApp):
    class Meta:
        label = 'bigqueryparser'
        base_controller = 'base'
        handlers = [BigQueryController]

with MyApp() as app:
    app.run()

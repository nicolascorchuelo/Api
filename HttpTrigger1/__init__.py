import logging

import azure.functions as func

import load_information as load




def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    method_type = req.method
    if method_type == "GET":
        pass

    elif method_type == "POST":
        try:
            req_body = req.get_json()
            name = req.params.get('file')
            load.create_table()
            func.HttpResponse("This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",status_code=200)
        except ValueError:
            name = None
        else:
            name = req_body.get('file')
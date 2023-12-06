# Lambda Function

import json
import logging
from urllib.parse import unquote_plus
import boto3
from helper import process_error, extract_kv, extract_lineitems, upload_to_s3, call_analyze_expense, combine_and_upload_to_s3, exponential_backoff_with_jitter, get_number_of_pages, initiate_async_processing
import time
import datetime


def lambda_handler(event, context):
    textract = boto3.client("textract")
    s3_client = boto3.client("s3")

    if event:
        start_time = time.time()
        timestamp = time.strftime("%Y-%m-%d-%H-%M-%S")
        file_obj = event["Records"][0]
        input_bucketname = str(file_obj["s3"]["bucket"]["name"])
        bucketname = "processedreciepts"
        filename = unquote_plus(str(file_obj["s3"]["object"]["key"]))
        file_base_name = filename.split("/")[-1].split(".")[0]

        # Check if the file is a PDF
        if filename.lower().endswith(".pdf"):
            # Get the number of pages in the PDF
            num_pages = get_number_of_pages(input_bucketname, filename)
            num_pages = int(num_pages)
            if num_pages > 1:
            # Start asynchronous processing for multi-page PDF
                initiate_async_processing(s3_client, temp_pdf_path, bucketname, filename)
            else:
        if temp_pdf_path:
                         os.remove(temp_pdf_path)
                # Process the file synchronously
                output_filename = f"{file_base_name}-{timestamp}-extracted.json"
                key = f"analyze-expense-output/{output_filename}"
    
                try:
                    max_retries = 5
                    base_delay = 0.5  # Initial delay in seconds (adjust this value based on your needs)
                    max_delay = 1.5   # Maximum delay in seconds (adjust this value based on your needs)
                    response = exponential_backoff_with_jitter(max_retries, base_delay, max_delay,
                                                               call_analyze_expense, input_bucketname, filename)
                   
                    for i in response["ExpenseDocuments"]:
                        try:
                            kv_data = extract_kv(i["SummaryFields"])
                            json_kv_data = json.dumps(kv_data, indent=2)
                            
                        except Exception as e:
                            error_msg = process_error()
                            logging.error(error_msg)
        
                        try:
                            lineitems_data = extract_lineitems(i["LineItemGroups"])
                            json_lineitems_data = json.dumps(lineitems_data, indent=2)
                            total_amount = sum(lineitems_data["price"])
                        except Exception as e:
                            error_msg = process_error()
                            logging.error(error_msg)
        
                        try:
                           
                            time_taken_seconds = time.time() - start_time
                            time_taken_formatted = str(datetime.timedelta(seconds=time_taken_seconds))
                            combine_and_upload_to_s3(s3_client, json_lineitems_data, json_kv_data, bucketname, f"{key}", time_taken_formatted, total_amount)
                        except Exception as e:
                            error_msg = process_error()
                            logging.error(error_msg)
        
                except Exception as e:
                    logging.error(e)
        
        else:
            # Process the file synchronously
            output_filename = f"{file_base_name}-{timestamp}-extracted.json"
            key = f"analyze-expense-output/{output_filename}"

            try:
                max_retries = 5
                base_delay = 0.5  # Initial delay in seconds (adjust this value based on your needs)
                max_delay = 1.5   # Maximum delay in seconds (adjust this value based on your needs)
                response = exponential_backoff_with_jitter(max_retries, base_delay, max_delay,
                                                           call_analyze_expense, input_bucketname, filename)
               
                for i in response["ExpenseDocuments"]:
                    try:
                        kv_data = extract_kv(i["SummaryFields"])
                        json_kv_data = json.dumps(kv_data, indent=2)
                        
                    except Exception as e:
                        error_msg = process_error()
                        logging.error(error_msg)
    
                    try:
                        lineitems_data = extract_lineitems(i["LineItemGroups"])
                        json_lineitems_data = json.dumps(lineitems_data, indent=2)
                        total_amount = sum(lineitems_data["price"])
                    except Exception as e:
                        error_msg = process_error()
                        logging.error(error_msg)
    
                    try:
                       
                        time_taken_seconds = time.time() - start_time
                        time_taken_formatted = str(datetime.timedelta(seconds=time_taken_seconds))
                        combine_and_upload_to_s3(s3_client, json_lineitems_data, json_kv_data, bucketname, f"{key}",time_taken_formatted, total_amount)
                    except Exception as e:
                        error_msg = process_error()
                        logging.error(error_msg)
    
            except Exception as e:
                logging.error(e)

    return {"statusCode": 200, "body": json.dumps("Code Ends")}

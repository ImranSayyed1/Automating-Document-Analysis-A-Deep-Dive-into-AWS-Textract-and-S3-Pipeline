# Helper Module

import sys
import io
import json
import traceback
from botocore.exceptions import ClientError
import boto3
import random
import time
import os
import logging
import PyPDF2
import fitz
from PIL import Image
import imagehash

def process_error():
    ex_type, ex_value, ex_traceback = sys.exc_info()
    traceback_string = traceback.format_exception(ex_type, ex_value, ex_traceback)
    error_msg = json.dumps(
        {
            "errorType": ex_type.__name__,
            "errorMessage": str(ex_value),
            "stackTrace": traceback_string,
        }
    )
    return error_msg

def call_analyze_expense(input_bucketname, filename):
    # Replace this function with your actual code that calls the "AnalyzeExpense" API
    textract = boto3.client("textract")
    response = textract.StartExpenseAnalysis(
        Document={
            "S3Object": {
                "Bucket": input_bucketname,
                "Name": filename,
            }
        }
    )
    return response

def exponential_backoff_with_jitter(max_retries, base_delay, max_delay, api_call, *args, **kwargs):
    retries = 0
    while retries < max_retries:
        try:
            response = api_call(*args, **kwargs)
            return response
        except ClientError as e:
            if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                retries += 1
                # Calculate the exponential backoff delay with jitter
                delay = min(base_delay * (2 ** retries) + random.uniform(0, base_delay), max_delay)
                time.sleep(delay)
            else:
                raise
    raise Exception("Max retries exceeded. Could not complete the operation.")

def get_number_of_pages(bucketname, filename):
    s3 = boto3.client("s3")
    try:
        # Download the PDF from S3
        temp_pdf_path = "/tmp/temp.pdf"
        s3.download_file(bucketname, filename, temp_pdf_path)

        # Open the PDF with PyPDF2
        with open(temp_pdf_path, 'rb') as pdf_file:
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            num_pages = len(pdf_reader.pages)

        return num_pages
    return temp_pdf_path
    except Exception as e:
        error_msg = process_error()
        logging.error(error_msg)
        return None
    #finally:
        # Clean up by deleting the temporary PDF file
        #if temp_pdf_path:
            #os.remove(temp_pdf_path)

def initiate_async_processing(s3_client, temp_pdf_path, bucketname, filename):
    # Detect duplicate pages and extract images
    pdf_images = detect_duplicate_pages(temp_pdf_path)
    if temp_pdf_path:
       os.remove(temp_pdf_path)
    # Process images using Textract
    summary_data, lineitems_data, total_amount = process_images(pdf_images)

    # Combine data
    summary_data = combinator(summary)
    lineitems_data = combinator(line_items)
    combined_data = {
        "SummaryData": summary_data,
        "LineItemsData": lineitems_data,
        "TotalAmount": total_amount
    }

    # Convert combined data to JSON
    json_data = json.dumps(combined_data, indent=2)
    key = f"analyze-expense-output/{filename}-combined.json"
    upload_to_s3(s3_client, json_data, bucketname, key)


def detect_duplicate_pages(pdf_file_path):
    # Open the PDF file
    pdf_document = fitz.open(pdf_file_path)
    # Create a list to store image hashes and images
    image_hashes = []
    image_list = []
    # Iterate through each page of the PDF
    for page_number in range(pdf_document.page_count):
        # Get the page object
        page = pdf_document.load_page(page_number)

        # Convert the page to an image (e.g., in PNG format)
        image = page.get_pixmap()

        # Create a PIL Image from the PyMuPDF image
        pil_image = Image.frombytes("RGB", [image.width, image.height], image.samples)

        # Calculate the image hash
        hash_value = imagehash.average_hash(pil_image)

        # Check if the hash is already in the list
        if hash_value in image_hashes:
            print(f'Duplicate found on page {page_number + 1}')
            break
        else:
            image_hashes.append(hash_value)
            # Append the PIL Image to the list
            image_list.append(pil_image)

    # Close the PDF file
    pdf_document.close()
    return image_list

def process_images(images):
    extracted_summary = None
    extracted_line_items = []

    # Process each image using Textract
    # Create lists or dictionaries to accumulate results
    summary_data_list = []
    lineitems_data_list = []
    total_amount_list = []

    # Process each image using Textract
    for index, image in enumerate(images, start=1):
    # Convert PIL image to bytes
        with io.BytesIO() as img_buffer:
            image.save(img_buffer, format="JPEG")
            img_bytes = img_buffer.getvalue()

        # Detect expense information using Textract
        response = textract.analyze_expense(Document={'Bytes': img_bytes})
        
        # Extract summary and line items
        for i in response["ExpenseDocuments"]:
            kv_data = extract_kv(i["SummaryFields"])
            lineitems_data = extract_lineitems(i["LineItemGroups"])
            total_amount = sum(lineitems_data["price"])
            
            # Append results to lists or dictionaries
            summary_data_list.append(kv_data)
            lineitems_data_list.append(lineitems_data)
            total_amount_list.append(total_amount)

        # Process the accumulated results as needed
        # For example, you can calculate totals or combine data from all images
            total_summary_data = summary_data_list
            combined_lineitems_data = lineitems_data_list
            total_amount = sum(total_amount_list)

    # Now you can work with the aggregated results  
    return total_summary_data, combined_lineitems_data, total_amount

def combinator(data_items):
    combined_data = {}
    for data in data_items:
        for key, values in data.items():
            if key in combined_data:
                combined_data[key].extend(values)
            else:
                combined_data[key] = values
    return combined_data

def upload_to_s3(s3_client, data, BUCKET_NAME, key):
    s3_client.put_object(Body=data, Bucket=BUCKET_NAME, Key=key)

def extract_lineitems(lineitemgroups):
    items, price, qty, items_confidence, price_confidence, qty_confidence = [], [], [], [], [], []
    t_items, t_price, t_qty = None, None, None
    t_items_confidence, t_price_confidence, t_qty_confidence = None, None, None
    
    for lines in lineitemgroups:
        for item in lines["LineItems"]:
            for line in item["LineItemExpenseFields"]:
                if line.get("Type").get("Text") == "ITEM":
                    t_items = line.get("ValueDetection").get("Text", "")
                    t_items_confidence = line.get("ValueDetection").get("Confidence", "")

                if line.get("Type").get("Text") == "PRICE":
                    t_price = line.get("ValueDetection").get("Text", "")
                    t_price_confidence = line.get("ValueDetection").get("Confidence", "")

                if line.get("Type").get("Text") == "QUANTITY":
                    t_qty = line.get("ValueDetection").get("Text", "")
                    t_qty_confidence = line.get("ValueDetection").get("Confidence", "")
            
            # Remove commas from the price string and convert to float
            if t_price:
                t_price = t_price.replace(",", "")
                t_price = float(t_price)
            
            items.append(t_items if t_items else "")
            price.append(t_price if t_price else 0.0)
            qty.append(t_qty if t_qty else "")
            items_confidence.append(t_items_confidence if t_items_confidence else "")
            price_confidence.append(t_price_confidence if t_price_confidence else "")
            qty_confidence.append(t_qty_confidence if t_qty_confidence else "")
            t_items, t_price, t_qty = None, None, None
            t_items_confidence, t_price_confidence, t_qty_confidence = None, None, None

    return {
        "items": items,
        "price": price,
        "quantity": qty,
        "items_confidence": items_confidence,
        "price_confidence": price_confidence,
        "quantity_confidence": qty_confidence
    }

def extract_kv(summaryfields):
    field_type, label, value, key_confidence, value_confidence = [], [], [], [], []
    for item in summaryfields:
        try:
            field_type.append(item.get("Type").get("Text", ""))
        except:
            field_type.append("")
        try:
            label.append(item.get("LabelDetection", "").get("Text", ""))
        except:
            label.append("")
        try:
            value.append(item.get("ValueDetection", "").get("Text", ""))
        except:
            value.append("")
        try:
            key_confidence.append(item.get("LabelDetection", "").get("Confidence", ""))
        except:
            key_confidence.append("")
        try:
            value_confidence.append(item.get("ValueDetection", "").get("Confidence", ""))
        except:
            value_confidence.append("")

    return {
        "Type": field_type,
        "Key": label,
        "Value": value,
        "KeyConfidence": key_confidence,
        "ValueConfidence": value_confidence
    }

def combine_and_upload_to_s3(s3_client, data1, data2, BUCKET_NAME, key, total_time, total_amt):
    # Combine both extracted data into one dictionary
    combined_data = {
        "lineitems": data1,
        "kv_data": data2,
        "time_taken": total_time,
        "Total_Amount": round(total_amt, 2)
    }
        # Convert the combined data to JSON string with indentation
    json_data = json.dumps(combined_data, indent=2)
    
    # Upload the combined JSON data to S3
    upload_to_s3(s3_client, json_data, BUCKET_NAME, key)


from flask import Flask, request, jsonify
import smartsheet
import os
import json
import logging
from datetime import datetime
import threading
import pyodbc
import sys

app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

# Configuration details
CONFIG = {
    'logLevel': 'info',
    'smartsheetAccessToken': 'mCV8xIWq5H4IcJmtjABxKDVEYxBrbAeFcZUjV',
    'sheetId': '8199960751198084',
    'webhookName': 'Test_webhook',
    'callbackUrl': 'your_callback_url'
}

# Initialize logging
def initialize_logging(log_level):
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
    logging.basicConfig(level=numeric_level)

# Initialize Smartsheet client
smartsheet_client = None

def initialize_smartsheet_client(token):
    global smartsheet_client
    smartsheet_client = smartsheet.Smartsheet(token)
    smartsheet_client.errors_as_exceptions(True)

# Check that we can access the sheet
def probe_sheet(target_sheet_id):
    logging.info(f'Checking for sheet id: {target_sheet_id}')
    sheet = smartsheet_client.Sheets.get_sheet(target_sheet_id, page_size=1)
    logging.info(f'Found sheet: "{sheet.name}" at {sheet.permalink}')

# A webhook only needs to be created once
# This method looks for an existing matching hook to reuse, else creates a new one
def initialize_hook(target_sheet_id, hook_name, callback_url):
    try:
        webhook = None

        # Get *all* my hooks
        response = smartsheet_client.Webhooks.list_webhooks(include_all=True)
        hooks = response.data
        logging.info(f'Found {len(hooks)} hooks owned by user')

        # Check for existing hooks on this sheet for this app
        for hook in hooks:
            if (hook.scope_object_id == target_sheet_id
                and hook.name == hook_name):
                webhook = hook
                logging.info(f'Found matching hook with id: {webhook.id}')
                break

        if not webhook:
            # Can't use any existing hook - create a new one
            webhook_body = smartsheet.models.Webhook({
                'name': hook_name,
                'callbackUrl': callback_url,
                'scope': 'sheet',
                'scopeObjectId': target_sheet_id,
                'events': ['*.*'],
                'version': 1
            })
            create_response = smartsheet_client.Webhooks.create_webhook(webhook_body)
            webhook = create_response.result
            print(webhook)
            logging.info(f'Created new hook: {webhook.id}')

        # Make sure webhook is enabled and pointing to our current url
        update_body = smartsheet.models.Webhook({
            'callbackUrl': callback_url,
            'enabled': True
        })
        update_response = smartsheet_client.Webhooks.update_webhook(webhook.id, update_body)
        updated_webhook = update_response.result
        logging.info(f'Hook enabled: {updated_webhook.enabled}, status: {updated_webhook.status}')
    except smartsheet.exceptions.ApiError as err:
        logging.error(err)

# Function to establish connection to SQL Server using Windows authentication
def connect_to_sql_server(server, database):
    try:
        connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';Trusted_Connection=yes;')
        return connection.cursor(), connection
    except pyodbc.Error as ex:
        logging.error("SQL Connection Error: %s", ex)
        sys.exit(1)

# Function to perform database operations
def perform_database_operation(cursor, connection, operation, rowid, data):
    try:
        if operation == "created":
            cursor.execute("INSERT INTO Account (Branch, Account, Name, PIC, [In Filter], [In Branch Key], Comment, Id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", data)
            connection.commit()
            logging.info("Row inserted successfully!")
        elif operation == "updated":
            cursor.execute("UPDATE Account SET Branch = ?, Account = ?, Name = ?, PIC = ?, [In Filter] = ?, [In Branch Key] = ?, Comment = ? WHERE Id = ?", data)
            connection.commit()
            logging.info("Row updated successfully!")
        elif operation == "deleted":
            cursor.execute("DELETE FROM Account WHERE Id = ?", rowid)
            connection.commit()
            logging.info("Row deleted successfully!")
        else:
            logging.info("Invalid operation specified.")
    except pyodbc.Error as ex:
        logging.error("SQL Operation Error: %s", ex)

# This method receives the webhook callbacks from Smartsheet
@app.route("/", methods=['POST'])
def webhook_callback():
    try:
        body = request.get_json()

        # Callback could be due to validation, status change, or actual sheet change events
        if 'challenge' in body:
            logging.info("Received verification callback")
            # Respond to the verification challenge by echoing the 'challenge' value back
            return jsonify({'smartsheetHookResponse': body['challenge']}), 200
        elif 'events' in body:
            logging.info(f'Received event callback with {len(body["events"])} events at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
            event_data = body  # Store the body
            response = '', 200  # Prepare the response

            def process_events_later():
                processed_event_ids.clear()
                process_events(event_data)

            threading.Thread(target=process_events_later).start()  # Process events in a separate thread

            return response  # Return response immediately
        elif 'newWebHookStatus' in body:
            logging.info(f'Received status callback, new status: {body["newWebHookStatus"]}')
            return '', 200
        else:
            logging.info(f'Received unknown callback: {body}')
            return '', 200
    except Exception as error:
        logging.error(error)
        return f'Error: {error}', 500


processed_event_ids = set()

def process_events(callback_data):
    if callback_data['scope'] != 'sheet':
        return
    # SQL Server details
    server = '.'
    database = 'NSH1'
    # Establishing connection to SQL Server
    cursor, connection = connect_to_sql_server(server, database)

    for event in callback_data['events']:
        if event['objectType'] != 'row':
            continue
        
        logging.info(f'Row: {event["eventType"]}, row id: {event["id"]}')
        try:
            if event['eventType'] == 'deleted':
                logging.info('Row deleted in SS')
                perform_database_operation(cursor, connection, 'deleted', event['id'], [])
            elif event['eventType'] in ['updated', 'created']:
                smartsheet_row = smartsheet_client.Sheets.get_row(
                    8199960751198084,  # Assuming event.sheetId contains the sheet ID
                    event['id']  # Use the row ID from the webhook event
                )
                event_id = f'{smartsheet_row.id}_{smartsheet_row.version}'
                print(event_id)
                if event_id in processed_event_ids:
                    continue
        
                processed_event_ids.add(event_id)
                values = [f"{a.value}" for a in smartsheet_row.cells]
                values.append(str(event['id']))
                output = [elem.strip() for elem in values]
                
                operation_type = 'updated' if event['eventType'] == 'updated' else 'created'
                logging.info(f'Row {operation_type} in SS')
                perform_database_operation(cursor, connection, operation_type, event['id'], output)
        except smartsheet.exceptions.ApiError as error:
            logging.error(f'Error fetching row: {error.message}')
        except IndexError as index_error:
            logging.error(f'Error processing row data: {index_error}')

    # Closing cursor and connection
    cursor.close()
    connection.close()


def delayed_initialization():
    initialize_hook(CONFIG['sheetId'], CONFIG['webhookName'], CONFIG['callbackUrl'])

if __name__ == '__main__':
    PORT = os.getenv('PORT', 3000)
    initialize_logging(CONFIG['logLevel'])
    initialize_smartsheet_client(CONFIG['smartsheetAccessToken'])
    probe_sheet(CONFIG['sheetId'])

    app.config['CONFIG'] = CONFIG

    # Start the Flask server
    threading.Timer(1, delayed_initialization).start()
    app.run(host='0.0.0.0', port=PORT)

# from flask import Flask, request, jsonify, Response
# from e2 import ElasticsearchBackend  
# import logging
# from uuid import uuid4
# from time import time
# from flask_cors import CORS
# import csv
# from io import StringIO
# from datetime import datetime


# app = Flask(__name__)
# CORS(app)

# es_backend = ElasticsearchBackend(["http://apm-logging-es.internal.olympus-world.zetaapps.in:9200"])


# logging.basicConfig(level=logging.INFO)

# @app.route('/alerts', methods=['GET'])
# def fetch_alerts():
#     processing_start_time = time()  # Renamed to avoid conflict
#     responder_name = request.args.get('responder_name', 'olympus_middleware_sre')
#     start_date = request.args.get('start_date', datetime.utcnow().strftime('%Y-%m-%d'))
#     end_date = request.args.get('end_date', datetime.utcnow().strftime('%Y-%m-%d'))

#     # Default start_time to start of the day if not provided
#     alert_start_time = request.args.get('start_time', '00:00:00')  # Renamed to alert_start_time
    
#     # If end_time is not provided, default to the current UTC time
#     if 'end_time' in request.args:
#         alert_end_time = request.args.get('end_time')
#     else:
#         alert_end_time = datetime.utcnow().strftime('%H:%M:%S')  # Current time as end time

#     # Construct start_datetime and end_datetime in ISO 8601 format
#     start_datetime = f"{start_date}T{alert_start_time}.000Z"
#     end_datetime = f"{end_date}T{alert_end_time}.999Z"  

#     request_id = str(uuid4()) 

#     if not es_backend.es:  # Check Elasticsearch connection
#         logging.error("Failed to connect to Elasticsearch")
#         return jsonify({"error": "Failed to connect to Elasticsearch"}), 500

#     try:
#         alerts = es_backend.get_alerts(responder_name, start_datetime, end_datetime)
#         if not alerts:
#             return jsonify({"message": "No alerts found"}), 404

#         readable_alerts = [es_backend.map_field_names(es_backend.flatten_json(alert)) for alert in alerts]

#         processing_end_time = time()  # Use a different variable for end time of processing
#         processing_time = processing_end_time - processing_start_time
#         count = len(readable_alerts)
#         logging.info(f"Returning {count} alerts with detailed fields")
#         response = {
#             "request_id": request_id,
#             "took": processing_time,
#             "data": readable_alerts,
#             "count": count
#         }

#         return jsonify(response), 200
#     except Exception as e:
#         logging.error(f"Error fetching alerts: {str(e)}")
#         return jsonify({"error": "Error processing alerts"}), 500
    

    
# @app.route('/alerts_csv', methods=['GET'])
# def fetch_alerts_csv():
#     processing_start_time = time()
#     responder_name = request.args.get('responder_name', 'olympus_middleware_sre')
#     start_date = request.args.get('start_date', datetime.utcnow().strftime('%Y-%m-%d'))
#     end_date = request.args.get('end_date', datetime.utcnow().strftime('%Y-%m-%d'))

#     # Default start_time to start of the day if not provided
#     alert_start_time = request.args.get('start_time', '00:00:00')
    
#     # If end_time is not provided, default to the current UTC time
#     if 'end_time' in request.args:
#         alert_end_time = request.args.get('end_time')
#     else:
#         alert_end_time = datetime.utcnow().strftime('%H:%M:%S')  # Current time as end time

#     # Construct start_datetime and end_datetime in ISO 8601 format
#     start_datetime = f"{start_date}T{alert_start_time}.000Z"
#     end_datetime = f"{end_date}T{alert_end_time}.999Z"  

#     if not es_backend.es:  # Check Elasticsearch connection
#         return "Failed to connect to Elasticsearch", 500

#     try:
#         alerts = es_backend.get_alerts(responder_name, start_datetime, end_datetime)
#         if not alerts:
#             return "No alerts found", 404

#         # Determine all unique columns across all alerts
#         all_columns = set()
#         for alert in alerts:
#             alert_flat = es_backend.map_field_names(es_backend.flatten_json(alert))
#             all_columns.update(alert_flat.keys())
        
#         all_columns = list(all_columns)  # Convert set to list to keep column order consistent

#         # Convert alerts to CSV
#         si = StringIO()
#         cw = csv.writer(si)
        
#         # Write headers
#         cw.writerow(all_columns)
        
#         # Write the data rows
#         for alert in alerts:
#             alert_flat = es_backend.map_field_names(es_backend.flatten_json(alert))
#             row = [alert_flat.get(column, '') for column in all_columns]  # Use empty string for missing values
#             cw.writerow(row)

#         output = si.getvalue()
#         si.close()

#         return Response(
#             output,
#             mimetype="text/csv",
#             headers={"Content-disposition": "attachment; filename=alerts.csv"})
#     except Exception as e:
#         logging.error(f"Error fetching alerts CSV: {str(e)}")
#         return "Error processing alerts CSV", 500


# if __name__ == "__main__":
#     app.run(debug=True, port=5000)
from flask import Flask, request, jsonify, Response
from e2 import ElasticsearchBackend  
import logging
import pytz
from uuid import uuid4
from time import time
from flask_cors import CORS
import csv
from io import StringIO
from datetime import datetime

app = Flask(__name__)
CORS(app)

es_backend = ElasticsearchBackend(["http://apm-logging-es.internal.olympus-world.zetaapps.in:9200"])

@app.route('/alerts', methods=['GET'])
def fetch_alerts():
    processing_start_time = time()
    responder_name = request.args.get('responder_name', default='olympus_middleware_sre')
    
    # Set timezone to IST
    ist_timezone = pytz.timezone('Asia/Kolkata')
    
    # Default start_date to today's date in IST
    start_date = request.args.get('start_date', default=datetime.now(ist_timezone).strftime('%Y-%m-%d'))
    
    # Default end_date to today's date in IST
    end_date = request.args.get('end_date', default=datetime.now(ist_timezone).strftime('%Y-%m-%d'))
    
    # Default start_time to 00:00:00 of the current day in IST
    start_time = request.args.get('start_time', default='00:00:00')
    
    # Default end_time to the current time in IST
    end_time = request.args.get('end_time', default=datetime.now(ist_timezone).strftime('%H:%M:%S'))

    try:
        alerts = es_backend.get_alerts(responder_name, start_date, end_date, start_time, end_time)
        if not alerts:
            return jsonify({"message": "No alerts found"}), 404

        readable_alerts = [es_backend.map_field_names(es_backend.flatten_json(alert)) for alert in alerts]

        processing_end_time = time()
        processing_time = processing_end_time - processing_start_time
        count = len(readable_alerts)

        response = {
            "request_id": str(uuid4()),
            "took": processing_time,
            "data": readable_alerts,
            "count": count
        }

        return jsonify(response), 200
    except Exception as e:
        logging.error(f"Error fetching alerts: {e}")
        return jsonify({"error": "Error processing alerts"}), 500

@app.route('/responder_names', methods=['GET'])
def get_responder_names():
    try:
        unique_responder_names = es_backend.get_unique_responder_names()
        return jsonify({"responder_names": unique_responder_names}), 200
    except Exception as e:
        logging.error(f"Error fetching unique responder names: {e}")
        return jsonify({"error": "Error fetching unique responder names"}), 500

@app.route('/alerts_csv', methods=['GET'])
def fetch_alerts_csv():
    processing_start_time = time()
    responder_name = request.args.get('responder_name', default='olympus_middleware_sre')
    
    # Set timezone to IST
    ist_timezone = pytz.timezone('Asia/Kolkata')
    
    # Default start_date to today's date in IST
    start_date = request.args.get('start_date', default=datetime.now(ist_timezone).strftime('%Y-%m-%d'))
    
    # Default end_date to today's date in IST
    end_date = request.args.get('end_date', default=datetime.now(ist_timezone).strftime('%Y-%m-%d'))
    
    # Default start_time to 00:00:00 of the current day in IST
    start_time = request.args.get('start_time', default='00:00:00')
    
    # Default end_time to the current time in IST
    end_time = request.args.get('end_time', default=datetime.now(ist_timezone).strftime('%H:%M:%S'))

    try:
        alerts = es_backend.get_alerts(responder_name, start_date, end_date, start_time, end_time)
        if not alerts:
            return jsonify({"message": "No alerts found"}), 404

        readable_alerts = [es_backend.map_field_names(es_backend.flatten_json(alert)) for alert in alerts]

        processing_end_time = time()
        processing_time = processing_end_time - processing_start_time
        count = len(readable_alerts)

        response = {
            "request_id": str(uuid4()),
            "took": processing_time,
            "data": readable_alerts,
            "count": count
        }

        # Determine all unique columns across all alerts
        all_columns = set()
        for alert in alerts:
            alert_flat = es_backend.map_field_names(es_backend.flatten_json(alert))
            all_columns.update(alert_flat.keys())
        
        all_columns = list(all_columns)  # Convert set to list to keep column order consistent

        # Convert alerts to CSV
        si = StringIO()
        cw = csv.writer(si)
        
        # Write headers
        cw.writerow(all_columns)
        
        # Write the data rows
        for alert in alerts:
            alert_flat = es_backend.map_field_names(es_backend.flatten_json(alert))
            row = [alert_flat.get(column, '') for column in all_columns]  # Use empty string for missing values
            cw.writerow(row)

        output = si.getvalue()
        si.close()

        return Response(
            output,
            mimetype="text/csv",
            headers={"Content-disposition": "attachment; filename=alerts.csv"})
    except Exception as e:
        logging.error(f"Error fetching alerts CSV: {str(e)}")
        return "Error processing alerts CSV", 500


if __name__ == "__main__":
     app.run(host='0.0.0.0, port=5000)

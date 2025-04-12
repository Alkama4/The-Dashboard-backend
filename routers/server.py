# External imports
from collections import defaultdict
from datetime import timedelta
import psutil
from fastapi import HTTPException, Query, APIRouter

# Internal imports
from utils import query_mysql

# Create the router object for this module
router = APIRouter()


@router.get("/drives_status")
def get_server_drives_info():
    try:
        # Define each folder with a name and path
        driveFolders = [
            {"name": "Boot drive", "path": "/driveBoot"},
            {"name": "Mass storage drive", "path": "/driveData"}
        ]
        drive_info = []

        for folder in driveFolders:
            # Get disk usage for the folder's path
            usage = psutil.disk_usage(folder["path"])

            drive_info.append({
                "name": folder["name"],
                "total": usage.total,
                "used": usage.used,
                "free": usage.free,
            })

        return drive_info
    except Exception as e:
        return {"error": str(e)}


@router.get("/logs/resource/get")
def get_server_resource_logs(timeframe: str = Query(None)):
    try:
        # Build the WHERE clause based on the timeframe
        timeframe_switch = {
            "24h": "24 HOUR",
            "12h": "12 HOUR",
            "6h": "6 HOUR",
            "3h": "3 HOUR",
            "1h": "1 HOUR",
            "30m": "30 MINUTE",
            "15m": "15 MINUTE"
        }
        if timeframe in timeframe_switch:
            where_clause = f"timestamp >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL {timeframe_switch[timeframe]})"
        else:
            raise HTTPException(status_code=400, detail="Invalid timeframe")

        # SQL query with dynamic WHERE clause
        server_data_query = f"""
            SELECT 
                cpu_temperature,
                cpu_usage,
                ram_usage,
                system_load,
                network_sent_bytes,
                network_recv_bytes,
                timestamp
            FROM server_resource_logs
            WHERE {where_clause}
        """
        
        # Execute the query and fetch the result
        server_data_result = query_mysql(server_data_query, ())

        # Format the results and normalize timestamps to 10-second precision
        formatted_data = []
        for result in server_data_result:
            formatted_data.append({
                "cpu_temperature": result[0],
                "cpu_usage": result[1],
                "ram_usage": result[2],
                "system_load": result[3],
                "network_sent_bytes": result[4],
                "network_recv_bytes": result[5],
                # Normalize timestamp to the nearest 10 seconds downwards
                "timestamp": result[6].replace(second=(result[6].second // 10) * 10, microsecond=0)
            })
        
        # Get the first and last timestamps from the result
        if formatted_data:
            start_time = formatted_data[0]['timestamp']
            end_time = formatted_data[-1]['timestamp']
        else:
            return {"data": []}

        # Generate the full range of timestamps in 10-second intervals from start to end
        current_time = start_time
        complete_data = []
        
        while current_time <= end_time:
            # Check if we have a log entry for the current timestamp
            matching_data = next((data for data in formatted_data if data['timestamp'] == current_time), None)
            
            if matching_data:
                complete_data.append(matching_data)
            else:
                # If no entry exists, append an entry with zero values
                complete_data.append({
                    "cpu_temperature": 0,
                    "cpu_usage": 0,
                    "ram_usage": 0,
                    "system_load": 0,
                    "network_sent_bytes": complete_data[-1]["network_sent_bytes"],
                    "network_recv_bytes": complete_data[-1]["network_recv_bytes"],
                    "timestamp": current_time
                })
            
            # Move to the next 10-second interval
            current_time += timedelta(seconds=10)
        
        return {"data": complete_data}
    
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/logs/fastapi/get")
def get_fastapi_request_data(timeframe: str = Query(None)):
    try:
        # Validate timeframe
        interval_map = {"24h": "24 HOUR", "7d": "7 DAY", "30d": "30 DAY"}
        if timeframe not in interval_map:
            raise HTTPException(status_code=400, detail="Invalid timeframe")
        interval = interval_map[timeframe]

        # Fetch logs from the database
        query = f"""
            SELECT status_code, method, endpoint, client_ip, backend_time_ms, UNIX_TIMESTAMP(timestamp) DIV 60 AS minute_bucket
            FROM server_fastapi_request_logs
            WHERE timestamp >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL {interval})
        """
        results = query_mysql(query, ())

        # Initialize data structures
        status_count = defaultdict(int)
        method_count = defaultdict(int)
        endpoint_count = defaultdict(int)
        client_ip_count = defaultdict(int)
        minute_buckets = defaultdict(int)
        backend_times = []
        endpoint_times = defaultdict(lambda: {"total_time": 0, "count": 0})

        # Track error codes per endpoint (for 400s and 500s)
        endpoint_error_codes = defaultdict(lambda: defaultdict(int))

        total_requests = 0
        total_backend_time = 0

        # Process logs
        for row in results:
            status_code, method, endpoint, client_ip, backend_time_ms, minute_bucket = row
            if endpoint == "/store_server_resource_logs":
                continue  # Skip this endpoint

            status_count[status_code] += 1
            method_count[method] += 1
            endpoint_count[endpoint] += 1
            client_ip_count[client_ip] += 1
            minute_buckets[minute_bucket] += 1

            total_requests += 1
            total_backend_time += backend_time_ms or 0

            if backend_time_ms is not None:
                backend_times.append(backend_time_ms)
                endpoint_times[endpoint]["total_time"] += backend_time_ms
                endpoint_times[endpoint]["count"] += 1

            # Track error codes for endpoints (400s and 500s)
            if 400 <= status_code < 600:
                endpoint_error_codes[endpoint][status_code] += 1

        # Calculate backend time histogram
        bucket_size = 100
        max_bucket = 1000
        buckets = [(i * bucket_size, (i + 1) * bucket_size) for i in range(max_bucket // bucket_size)]
        buckets.append((max_bucket, 600000))

        histogram = defaultdict(int)
        for time in backend_times:
            for i, (start, end) in enumerate(buckets):
                if start <= time < end:
                    histogram[i] += 1
                    break

        histogram_data = [
            {
                "time_range": f"{start}ms - {end - 1}ms" if i < len(buckets) - 1 else f"{start}ms and up",
                "count": histogram[i]
            }
            for i, (start, end) in enumerate(buckets)
        ]


        # Fill missing minute buckets
        min_bucket = min(minute_buckets.keys(), default=0)
        max_bucket = max(minute_buckets.keys(), default=0)
        filled_minute_buckets = [
            {"minute_bucket": minute, "count": minute_buckets.get(minute, 0)}
            for minute in range(min_bucket, max_bucket + 1)
        ]

        # Calculate average backend time
        avg_backend_time = total_backend_time / total_requests if total_requests else 0

        # Prepare endpoint error summary
        endpoint_error_summary = []
        for endpoint, codes in endpoint_error_codes.items():
            error_counts = [{"status_code": code, "count": count} for code, count in codes.items()]
            endpoint_error_summary.append({
                "endpoint": endpoint,
                "errors": sorted(error_counts, key=lambda x: x['count'], reverse=True)
            })

        # Prepare response data
        return {
            "data": {
                "total_requests": total_requests,
                "avg_backend_time": avg_backend_time,
                "status_code": sorted(
                    [{"status_code": k, "count": v} for k, v in status_count.items()],
                    key=lambda x: x['count'], reverse=True
                ),
                "method_count": sorted(
                    [{"method": k, "count": v} for k, v in method_count.items()],
                    key=lambda x: x['count'], reverse=True
                ),
                "client_ip_count": sorted(
                    [{"client_ip": k, "count": v} for k, v in client_ip_count.items()],
                    key=lambda x: x['count'], reverse=True
                ),
                "endpoint_count": sorted(
                    [
                        {
                            "endpoint": endpoint,
                            "count": count,
                            "avg_response_time_ms": round(endpoint_times[endpoint]["total_time"] / endpoint_times[endpoint]["count"])
                            if endpoint in endpoint_times and endpoint_times[endpoint]["count"] > 0
                            else 0
                        }
                        for endpoint, count in endpoint_count.items()
                    ],
                    key=lambda x: x['count'], reverse=True
                ),
                "requests_over_time": filled_minute_buckets,
                "backend_time_histogram": histogram_data,
                "endpoint_error_summary": sorted(
                    endpoint_error_summary, key=lambda x: sum(err["count"] for err in x["errors"]), reverse=True
                ),
            }
        }

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/logs/resource/store")
def store_server_resource_logs(data: dict):
    try:
        # Insert the new log entry
        store_data_query = """
            INSERT INTO server_resource_logs (
                cpu_temperature,
                cpu_usage,
                ram_usage,
                system_load,
                network_sent_bytes,
                network_recv_bytes
            ) 
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        store_data_params = (
            data["cpu_temperature"],
            data["cpu_usage"],
            data["ram_usage"],
            data["system_load"],
            data["network_sent_bytes"],
            data["network_recv_bytes"],
        )
        query_mysql(store_data_query, store_data_params)

        return {"message": "Success"}

    except Exception as e:
        print(e)
        return {"error": str(e)}
    

# No endpoint for post fastapi logs since they 
# are automatically logged for each query


@router.post("/logs/cleanup")
def clean_up_logs():

    fastapi_cleaning_query = """
        DELETE FROM server_fastapi_request_logs
        WHERE timestamp < NOW() - INTERVAL 1 DAY;
    """
    query_mysql(fastapi_cleaning_query)

    server_resource_cleaning_query = """
        DELETE FROM server_resource_logs
        WHERE timestamp < NOW() - INTERVAL 1 DAY;
    """
    query_mysql(server_resource_cleaning_query)

    return {'message': 'Success!'}

# External imports
from collections import defaultdict
from datetime import timedelta, datetime
import psutil
from fastapi import HTTPException, Query, APIRouter

# Internal imports
from utils import query_mysql, format_time_difference

# Create the router object for this module
router = APIRouter()


@router.get("/drives")
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


@router.get("/logs/system_resources")
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


@router.post("/logs/system_resources")
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
    

@router.get("/logs/fastapi")
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


# No endpoint for post /logs/fastapi since they are 
# automatically logged for each query.


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


@router.post("/backups")
def log_backup(data: dict):

    # Check and get backup_name
    backup_name = data.get("backup_name")
    if not backup_name:
        raise HTTPException(status_code=403, detail="Missing backup_name param.")
    
    # Update the last_success column for the specified backup name
    query = """
        UPDATE backups
        SET last_success = NOW()
        WHERE backup_name = %s
    """
    query_mysql(query, (backup_name,))
    
    return {"message": "Logged successfully"}


@router.get("/backups")
def get_backups():
    query = "SELECT backup_id, backup_name, backup_direction, backup_category, peer_device, source_path, destination_path, last_success FROM backups"
    
    backups = query_mysql(query)
    
    if backups:
        formatted_backups = defaultdict(list)  # Group backups by category
        for backup in backups:
            # Parse the last_success timestamp
            last_success = backup[7]
            if last_success:
                time_since = datetime.now() - last_success
                last_success_time_since = format_time_difference(time_since)
                last_success_in_hours = round(time_since.total_seconds() / 3600, 2)

                # A custom thershold for the status when using the air gapped drive
                thresholds = {
                    "Old laptop hdd": [24 * 7 * (52 / 4), 24 * 7 * (52 / 2)],
                    "default": [24, 72]
                }
                device_thresholds = thresholds.get(backup[1], thresholds["default"])

                if last_success_in_hours < device_thresholds[0]:
                    status = "good"
                elif last_success_in_hours < device_thresholds[1]:
                    status = "warning"
                else:
                    status = "bad"
            else:
                last_success_time_since = "Never"
                status = "bad"
            
            # Format the backup direction (schedule)
            direction = backup[2]
            if backup[1] == 'Old laptop hdd':
                schedule = "-"
            elif direction == 'up':
                schedule = "Päivittäin, 4.00"  # Daily at 4.00
            elif direction == 'down':
                schedule = "Päivittäin, 5.00"  # Daily at 5.00
            else:
                schedule = "-"
            
            # Calculate time until the next backup
            if (backup[1] != 'Old laptop hdd'):
                now = datetime.now()
                if direction == 'up':
                    # Next backup scheduled for 4:00 AM today or tomorrow
                    next_backup_time = datetime(now.year, now.month, now.day, 4, 0)  # 4:00 AM today
                    if now > next_backup_time:
                        # If it's already past 4:00 AM, schedule for 4:00 AM tomorrow
                        next_backup_time += timedelta(days=1)
                elif direction == 'down':
                    # Next backup scheduled for 5:00 AM today or tomorrow
                    next_backup_time = datetime(now.year, now.month, now.day, 5, 0)  # 5:00 AM today
                    if now > next_backup_time:
                        # If it's already past 5:00 AM, schedule for 5:00 AM tomorrow
                        next_backup_time += timedelta(days=1)
                else:
                    return "Invalid direction"  # In case of an unexpected direction

                # Calculate the time difference between now and the next scheduled time
                time_until_next = next_backup_time - now
                time_until_next_str = format_time_difference(time_until_next)
            else:
                time_until_next_str = "-"
            
            # Format the backup data
            formatted_backup = {
                "backup_name": backup[1],
                "backup_direction": direction,
                "peer_device": backup[4],
                "schedule": schedule,
                "status": status,
                # Paths
                "source_path": backup[5],
                "destination_path": backup[6],
                # Times since and until
                "last_success_time_since": last_success_time_since,
                "time_until_next": time_until_next_str,
            }
            
            # Group backups by their category
            formatted_backups[backup[3]].append(formatted_backup)
        
        return {"backups": formatted_backups}
    else:
        raise HTTPException(status_code=404, detail="No backups found.")


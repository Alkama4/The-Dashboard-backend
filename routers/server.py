# External imports
from collections import defaultdict
from datetime import timedelta, datetime, timezone
import json
import psutil
from fastapi import HTTPException, Query, APIRouter

# Internal imports
from utils import query_mysql, format_time_difference, redis_client

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
async def get_server_resource_logs(timeframe: str = Query(None)):
    try:
        timeframe_switch = {
            "24h": 86400,
            "12h": 43200,
            "6h": 21600,
            "3h": 10800,
            "1h": 3600,
            "30m": 1800,
            "15m": 900
        }

        if timeframe not in timeframe_switch:
            raise HTTPException(status_code=400, detail="Invalid timeframe")

        seconds_limit = timeframe_switch[timeframe]
        now = datetime.now(timezone.utc)  # This gives a timezone-aware datetime in UTC

        logs = await redis_client.lrange("system_resource_logs", 0, -1)
        parsed_logs = [json.loads(log) for log in logs]

        # Filter and normalize timestamps
        filtered_logs = []
        for entry in parsed_logs:
            ts = datetime.fromisoformat(entry["timestamp"])  # Assuming it's naive initially
            ts = ts.replace(tzinfo=timezone.utc)  # Convert to timezone-aware (UTC)

            if (now - ts).total_seconds() <= seconds_limit:
                # Normalize timestamp to nearest 5 seconds
                entry["timestamp"] = ts.replace(second=(ts.second // 5) * 5, microsecond=0)
                filtered_logs.append(entry)

        # Sort by timestamp ascending
        filtered_logs.sort(key=lambda x: x["timestamp"])

        if not filtered_logs:
            return {"data": []}

        start_time = filtered_logs[0]["timestamp"]
        end_time = filtered_logs[-1]["timestamp"]
        current_time = start_time
        complete_data = []

        while current_time <= end_time:
            match = next((item for item in filtered_logs if item["timestamp"] == current_time), None)
            if match:
                complete_data.append(match)
            else:
                complete_data.append({
                    "cpu_temperature": 0,
                    "cpu_usage": 0,
                    "ram_usage": 0,
                    "cpu_clock_mhz": 0,
                    "uptime_seconds": 0,
                    "network_sent_bytes": complete_data[-1]["network_sent_bytes"] if complete_data else 0,
                    "network_recv_bytes": complete_data[-1]["network_recv_bytes"] if complete_data else 0,
                    "timestamp": current_time
                })
            current_time += timedelta(seconds=5)  # Step forward in 5-second increments

        return {"data": complete_data}

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/logs/system_resources")
async def store_server_resource_logs(data: dict):
    try:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **data
        }

        # One log every 5 seconds
        # 24 * 60 * 12 = 17280
        MAX_LOGS_AMOUNT = 17280

        await redis_client.lpush("system_resource_logs", json.dumps(log_entry))
        await redis_client.ltrim("system_resource_logs", 0, MAX_LOGS_AMOUNT - 1)

        return {"message": "Success"}

    except Exception as e:
        print(e)
        return {"error": str(e)}


@router.get("/logs/fastapi")
async def get_fastapi_request_data(timeframe: str = Query(None)):
    try:
        # Validate and map timeframe to seconds
        interval_map = {"24h": 86400, "7d": 604800, "30d": 2592000}
        if timeframe not in interval_map:
            raise HTTPException(status_code=400, detail="Invalid timeframe")

        now = datetime.now(timezone.utc)

        # Fetch and parse logs from Redis
        logs = await redis_client.lrange("fastapi_request_logs", 0, -1)
        parsed_logs = [json.loads(log) for log in logs]

        # Filter logs based on timeframe and calculate minute bucket
        filtered = []
        for log in parsed_logs:
            ts = datetime.fromisoformat(log["timestamp"])
            if (now - ts).total_seconds() <= interval_map[timeframe]:
                log["minute_bucket"] = int(ts.timestamp()) // 60
                filtered.append(log)

        # Initialize aggregation structures
        status_count = defaultdict(int)
        method_count = defaultdict(int)
        endpoint_count = defaultdict(int)
        client_ip_count = defaultdict(int)
        minute_buckets = defaultdict(int)
        endpoint_times = defaultdict(lambda: {"total_time": 0, "count": 0})
        endpoint_error_codes = defaultdict(lambda: defaultdict(int))

        backend_times = []
        total_requests = 0
        total_backend_time = 0

        # Process filtered logs and build stats
        for log in filtered:
            if log["endpoint"] == "/store_server_resource_logs":
                continue

            code = log["status_code"]
            method = log["method"]
            endpoint = log["endpoint"]
            ip = log["client_ip"]
            time_ms = log["backend_time_ms"]
            bucket = log["minute_bucket"]

            status_count[code] += 1
            method_count[method] += 1
            endpoint_count[endpoint] += 1
            client_ip_count[ip] += 1
            minute_buckets[bucket] += 1

            total_requests += 1
            total_backend_time += time_ms
            backend_times.append(time_ms)

            endpoint_times[endpoint]["total_time"] += time_ms
            endpoint_times[endpoint]["count"] += 1

            if 400 <= code < 600:
                endpoint_error_codes[endpoint][code] += 1

        # Build backend response time histogram (bucketed)
        bucket_size = 100
        max_bucket = 1000
        buckets = [(i * bucket_size, (i + 1) * bucket_size) for i in range(max_bucket // bucket_size)]
        buckets.append((max_bucket, 600000))

        histogram = defaultdict(int)
        for t in backend_times:
            for i, (start, end) in enumerate(buckets):
                if start <= t < end:
                    histogram[i] += 1
                    break

        histogram_data = [
            {
                "time_range": f"{start}ms - {end - 1}ms" if i < len(buckets) - 1 else f"{start}ms and up",
                "count": histogram[i]
            }
            for i, (start, end) in enumerate(buckets)
        ]

        # Fill in missing time buckets for graphing request volume over time
        min_bucket = min(minute_buckets.keys(), default=0)
        max_bucket = max(minute_buckets.keys(), default=0)
        filled_minute_buckets = [
            {"minute_bucket": minute, "count": minute_buckets.get(minute, 0)}
            for minute in range(min_bucket, max_bucket + 1)
        ]

        # Calculate average backend processing time
        avg_backend_time = total_backend_time / total_requests if total_requests else 0

        # Prepare error stats per endpoint
        endpoint_error_summary = [
            {
                "endpoint": endpoint,
                "errors": sorted(
                    [{"status_code": code, "count": count} for code, count in codes.items()],
                    key=lambda x: x["count"], reverse=True
                )
            }
            for endpoint, codes in endpoint_error_codes.items()
        ]

        # Final response payload with all the aggregated data
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
                            if endpoint_times[endpoint]["count"] > 0 else 0
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


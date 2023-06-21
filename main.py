from fastapi import FastAPI, Request
import pyodbc
import json
import requests
import mysql.connector
import uvicorn
import asyncio

app = FastAPI()

# Create connection to SQL Server (hiptime40 database)
def create_connection():
    connection = pyodbc.connect(
        "Driver={SQL Server};"
        "Server=IBM-BLDSVR-02\\HRMS;"
        "Database=hiptime40;"
        "UID=sa;"
        "PWD=P@ssw0rd@2;"
        "autocommit=True"
    )
    return connection

# Create connection to MySQL (udch_erp database)
def create_connection_line():
    connection = mysql.connector.connect(
        host="192.168.254.13",
        user="isarapong",
        password="Pwd@Isarapong",
        database="udch_erp"
    )
    return connection

# Run the connections asynchronously
async def run_create_connections():
    loop = asyncio.get_event_loop()
    connection1 = await loop.run_in_executor(None, create_connection)
    connection2 = await loop.run_in_executor(None, create_connection_line)
    return connection1, connection2

# Start the app and establish the database connections
@app.on_event("startup")
async def startup():
    app.db_connection, app.db_connection_line = await run_create_connections()

# Close the database connections on shutdown
@app.on_event("shutdown")
async def shutdown():
    await app.db_connection.close()
    await app.db_connection_line.close()

# Endpoint to retrieve test data by sendline
@app.get("/test/{sendline}")
async def get_test_data(sendline: int):
    try:
        cursor = app.db_connection.cursor()
        query = "SELECT * FROM test WHERE sendline = ?"
        cursor.execute(query, (sendline,))
        result = cursor.fetchall()
        cursor.close()

        if result:
            # Convert the result to a list of dictionaries
            data = []
            for item in result:
                data.append({
                    'enrollnumber': item[0],
                    'datetimescan': item[1],
                    'timetype': item[2],
                    'sendline': item[3],
                })

            # Convert the data to a JSON object
            json_data = json.dumps(data)

            # Iterate over the retrieved data
            for item in data:
                line_id = await get_line_token(item['enrollnumber'])
                if line_id:
                    # Print the line_id for verification
                    print("LINE ID:", line_id)

                    # Set the push message based on timetype and include datetimescan and enrollnumber
                    if item['timetype'] == 'IN':
                        push_message = f"ทดสอบระบบคุณได้เข้างาน\n {item['datetimescan']}\n {item['enrollnumber']}"
                    elif item['timetype'] == 'OUT':
                        push_message = f"ทดสอบระบบคุณได้ออกงาน\n {item['datetimescan']}\n {item['enrollnumber']}"

                    # Send the push message to LINE using the retrieved line_id and custom message
                    send_push_message(line_id, push_message, item['datetimescan'], item['enrollnumber'])

                    # Update the sendline value in the test table
                    update_sendline(item['enrollnumber'])
                else:
                    # Update the sendline value to 1 when line_id is not found
                    update_sendline(item['enrollnumber'])

                    # Send the enrollnumber to the Employee Number System
                    send_to_employee_number(item['enrollnumber'])

            return json_data
        else:
            return {"message": "Test not found"}

    except Exception as e:
        print(f"Error occurred while fetching test: {e}")
        return None


# Callback endpoint for processing received messages
@app.post("/callback")
async def callback(request: Request):
    # Parse the request body as JSON
    body = await request.json()

    # Process the received message
    # Add your own logic here

    # Return a response (e.g., 200 OK)
    return {"message": "OK"}


def send_push_message(line_id: str, message: str, datetimescan: str, enrollnumber: int):
    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer 2LboKqCiqiGcdMEX8tNax/xTyk9Dr3KZKtWnczG/cNnqgXRApxoMh/JTDEuzcXSskwiWfyXdDxQmGsF2tUx+4pROEyLQAwfDlxMDbunB+kBZVlj27Wh7QddDoDG/QovO3eD+3D2mJr0o+Cn+KGb8YgdB04t89/1O/w1cDnyilFU="
    }
    data = {
        "to": line_id,
        "messages": [
            {"type": "text", "text": message}
        ]
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()  # Raise an exception if the request fails
    except Exception as e:
        print(f"Error occurred while sending push message: {e}")


# Get the LINE token for the enrollnumber
async def get_line_token(enrollnumber: int):
    try:
        connection = app.db_connection_line
        cursor = connection.cursor()

        # Retrieve the line_id using the enrollnumber from the fastapi_line2 table
        query_line_id = "SELECT line_id FROM fastapi_line2 WHERE employee_code = %s"
        cursor.execute(query_line_id, (enrollnumber,))
        result_line_id = cursor.fetchone()

        # Fetch all pending results to avoid "Unread result found" error
        cursor.fetchall()

        cursor.close()

        if result_line_id:
            line_id = result_line_id[0]
            return line_id
        else:
            return None

    except Exception as e:
        print(f"Error occurred while retrieving LINE token: {e}")
        return None


# Update the sendline value in the test table
def update_sendline(enrollnumber: int):
    try:
        cursor = app.db_connection.cursor()
        query = "UPDATE test SET sendline = 1 WHERE enrollnumber = ?"
        cursor.execute(query, (enrollnumber,))
        cursor.commit()
        cursor.close()
    except Exception as e:
        print(f"Error occurred while updating sendline: {e}")


# Send the enrollnumber to the Employee Number System
def send_to_employee_number(enrollnumber: int):
    # Add your logic here to send the enrollnumber to the Employee Number System
    pass


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

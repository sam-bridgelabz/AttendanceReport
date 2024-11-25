# Attendance Report Script

## Overview

This script generates an attendance report by processing data from Google Sheets. It calculates time differences based on log times, manages candidate statuses, and updates attendance summaries in a structured manner.

## How to Run the Script

Ensure the script file is in the project directory. Run the script using the following command:

```bash
python attendance_report.py <location>
```

-   `<location>` is **mandatory**; failing to provide it will raise an exception and terminate the script.

## Prerequisites

### Service Account JSON File

-   The Google Sheets API requires a **service account JSON file** for authorization.
-   This file must be placed in the **project directory**.

### AttendanceSettings Sheet

-   All required Google Sheet and worksheet names must be specified in the **`AttendanceSettings`** sheet.
-   This sheet needs to be **updated on the first day of every month**.

### New Sheets Sharing

-   Each month, new Google Sheets are created for attendance.
-   These sheets must be **shared with the service account** to ensure access.

## Project Workflow

### Google Sheets API Authorization

The script authorizes the Google Sheets API using the **service account JSON file**.

### Fetching and Preparing Data

#### Login Response Scanner Sheet:

-   Data is fetched and converted to a pandas DataFrame.
-   The script calculates time differences based on log times.

#### The Master Sheet:

-   Data is fetched as a DataFrame.
-   Newly added and active candidates are filtered out.

### Data Merging

The login and master sheet data are merged based on **candidate email addresses**.

### LogReport Sheet

Maintains a record of logged dates and attendance data.

#### Contains:

-   **Last Logged Date Worksheet**:
    -   Tracks the logged dates for the month.
-   **Attendance Worksheet**:
    -   Adds a new column for each day to summarize the presence status and time spent at the office.

### Daily Worksheet Creation

Each day, a new worksheet is created in the **LogReport** sheet. The worksheet name is the **date**. It contains a detailed summary of **candidate login and logout times**.

## Attendance Statuses

| **Status**      | **Description**               |
| --------------- | ----------------------------- |
| **P-Login**     | Present, but login not done.  |
| **P-Logout**    | Present, but logout not done. |
| **A-Absent**    | Candidate is absent.          |
| **Dr-Dropped**  | Candidate has dropped out.    |
| **De-Deployed** | Candidate has been deployed.  |

## Key Points to Note

1. Ensure the **service account JSON file** is placed in the project directory and is accessible.
2. The **`AttendanceSettings`** sheet must be updated on the first day of every month to include the required sheet and worksheet names.
3. **Share the new monthly Google Sheets** with the service account to ensure proper functioning.

import pandas as pd
import mysql.connector
import hashlib
import random
import string
import sys
import os


# --- Password Generation ---
def generate_password(length=8):
    """Generate a secure 8-character password with mixed characters."""
    upper   = random.choice(string.ascii_uppercase)
    lower   = random.choice(string.ascii_lowercase)
    digit   = random.choice(string.digits)
    special = random.choice("!@#$%")
    all_chars = string.ascii_letters + string.digits + "!@#$%"
    rest = random.choices(all_chars, k=4)
    password_list = list(upper + lower + digit + special) + rest
    random.shuffle(password_list)
    return "".join(password_list)


def clean_phone(raw):
    """
    Normalize phone to format: 923XXXXXXXXX (12 digits, no +, no leading 0)
    Examples:
        +923175813489  -> 923175813489
        03175813489    -> 923175813489
        923175813489   -> 923175813489
        3175813489     -> 923175813489
    """
    digits = "".join(filter(str.isdigit, str(raw)))  # strip +, spaces, dashes
    if digits.startswith("0"):
        digits = digits[1:]                           # remove leading 0
    if not digits.startswith("92"):
        digits = "92" + digits                        # prepend 92 if missing
    return digits


def hash_password(plain_password):
    """Hash password using SHA-256."""
    return hashlib.sha256(plain_password.encode()).hexdigest()


# --- Role Type (stores the raw string e.g. RESS-Head, RESS) ---
def get_role_type(role_value):
    return str(role_value).strip()


# --- Manager ID Lookup ---
def get_manager_id(cursor, manager_name):
    """
    Look up manager by name in employees table and return their id.
    Returns None if not found.
    """
    if not manager_name or str(manager_name).strip().lower() == "nan":
        return None
    cursor.execute("SELECT id FROM employees WHERE name = %s LIMIT 1", (str(manager_name).strip(),))
    result = cursor.fetchone()
    if result:
        return result[0]
    print(f"  [WARN] Manager not found in DB: '{manager_name}' -> manager_id set to NULL")
    return None


# --- Email Template ---
def generate_email_template(name, username, plain_pw):
    """Generate the credential email body for a single employee."""
    separator = "=" * 55
    return f"""
{separator}
SUBJECT : SPM Portal Login Credentials

Dear {name},

Please find below your login credentials for the SPM portal.

URL       : https://track.zong.com.pk/spm/
User Name : {username}
Password  : {plain_pw}

"""


# --- Database Configuration ---
def get_db_connection():
    """Create and return a new MySQL database connection."""
    return mysql.connector.connect(
        host="***********",
        user="*****",
        password="**********",
        database="**********"
    )


# --- Main Import Function ---
def import_employees(excel_path):
    """Read Excel file and insert employees into the database."""

    print(f"Reading Excel file: {excel_path}")
    df = pd.read_excel(excel_path)

    print(f"Columns found: {list(df.columns)}")
    print(f"Total rows: {len(df)}\n")

    # Connect to DB
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        print("Database connected successfully.\n")
    except Exception as e:
        print(f"Database connection failed: {e}")
        sys.exit(1)

    insert_query = """
        INSERT INTO employees
            (employee_number, name, employee_email, phone_number,
             city, role_type, roles_id, manager_id, employee_username, employee_password)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    success_count = 0
    fail_count    = 0
    skipped_rows  = []

    # Logs
    log_lines    = ["Employee Number | Name | Username | Plain Password | Hashed Password\n"]
    log_lines.append("-" * 90 + "\n")
    email_drafts = []   # All email drafts saved to file

    for idx, row in df.iterrows():
        try:
            employee_number = str(row.get("Employee Number", "")).strip()
            name            = str(row.get("Name", "")).strip()
            email           = str(row.get("Email", "")).strip()
            mobile_no       = clean_phone(row.get("Mobile No", ""))
            username        = str(row.get("Username", "")).strip()
            city_id         = str(row.get("City ID", "")).strip()
            role_raw        = str(row.get("Role", "")).strip()
            reporting_to    = str(row.get("Reporting To", "")).strip()

            # Skip empty rows
            if not employee_number or employee_number == "nan":
                continue

            # Generate and hash password
            plain_pw  = generate_password()
            hashed_pw = hash_password(plain_pw)

            # role_type = raw string from excel (RESS-Head, RESS, etc.)
            role_type = get_role_type(role_raw)

            # roles_id = always 1 (readonly)
            roles_id = 1

            # manager_id = look up reporting_to name in DB
            manager_id = get_manager_id(cursor, reporting_to)

            values = (
                employee_number,
                name,
                email,
                mobile_no,
                city_id,
                role_type,
                roles_id,
                manager_id,
                username,
                hashed_pw
            )

            cursor.execute(insert_query, values)
            success_count += 1

            # Password log entry
            log_lines.append(
                f"{employee_number} | {name} | {username} | {plain_pw} | {hashed_pw}\n"
            )

            # --- Print DB insert confirmation ---
            print(f"[DB] Inserted: {employee_number} - {name}  |  role_type: {role_type}  |  roles_id: {roles_id}  |  manager_id: {manager_id}")
            print(f"[DB] Credentials — User: {username}  |  PW: {plain_pw}  |  Mobile: {mobile_no}")

            # --- Print email draft immediately after each insert ---
            email_body = generate_email_template(name, username, plain_pw)
            print(email_body)
            email_drafts.append(
                f"TO EMPLOYEE : {name}\n"
                f"TO EMAIL    : {email}\n"
                + email_body + "\n"
            )

        except Exception as e:
            fail_count += 1
            skipped_rows.append((idx + 2, str(e)))
            print(f"[ERROR] Row {idx + 2} failed: {e}\n")

    conn.commit()
    cursor.close()
    conn.close()

    # --- Save password log ---
    base_dir    = os.path.dirname(os.path.abspath(excel_path))
    log_path    = os.path.join(base_dir, "employee_passwords_log.txt")
    emails_path = os.path.join(base_dir, "employee_email_drafts.txt")

    with open(log_path, "w", encoding="utf-8") as f:
        f.writelines(log_lines)

    with open(emails_path, "w", encoding="utf-8") as f:
        f.writelines(email_drafts)

    print("\n" + "=" * 60)
    print(f"Successfully inserted : {success_count} employees")
    print(f"Failed rows           : {fail_count}")
    if skipped_rows:
        print("\nFailed rows (Excel row | Error):")
        for row_num, err in skipped_rows:
            print(f"  Row {row_num}: {err}")
    print(f"\nPassword log   saved -> {log_path}")
    print(f"Email drafts   saved -> {emails_path}")
    print("=" * 60)


# --- Entry Point ---
if __name__ == "__main__":

    # ----------------------------------------------------------------
    # OPTION A: Hardcode your file path here and just run the script
    #           with no arguments: python import_employees.py
    # ----------------------------------------------------------------
    HARDCODED_PATH = r"D:\python projects\bulk_upload_spm_users\nwd_hierarchy.xlsx"

    if len(sys.argv) >= 2:
        # OPTION B: Pass path as argument:
        # python import_employees.py "D:\path\to\file.xlsx"
        excel_file = sys.argv[1]
    else:
        excel_file = HARDCODED_PATH

    if not os.path.exists(excel_file):
        print(f"\nFile not found: {excel_file}")
        print("Check that the path is correct and the file exists.")
        sys.exit(1)

    import_employees(excel_file)

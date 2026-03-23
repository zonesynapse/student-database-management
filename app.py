from flask import Flask, render_template, request, redirect, url_for, jsonify, session
import sqlite3
import pandas as pd
import os

app = Flask(__name__)
DATABASE = "database.db"
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "student-management-dev-key")

REQUIRED_UPLOAD_COLUMNS = [
    "name",
    "department",
    "year_of_joining",
    "tenth_mark",
    "twelfth_mark",
    "aadhar_number",
    "pan_number",
]


def get_db_connection():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def generate_next_roll_number(conn):
    row = conn.execute(
        """
        SELECT MAX(CAST(roll_number AS INTEGER)) AS max_roll
        FROM students
        WHERE roll_number IS NOT NULL AND roll_number != ''
        """
    ).fetchone()
    next_roll = (row["max_roll"] or 0) + 1
    if next_roll > 999:
        raise ValueError("Maximum 3-digit roll number limit reached.")
    return f"{next_roll:03d}"


def init_db():
    conn = get_db_connection()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS students (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            roll_number TEXT UNIQUE,
            name TEXT NOT NULL,
            department TEXT NOT NULL,
            year_of_joining INTEGER NOT NULL,
            tenth_mark REAL NOT NULL,
            twelfth_mark REAL NOT NULL,
            aadhar_number TEXT NOT NULL,
            pan_number TEXT NOT NULL
        )
        """
    )

    # Ensure older databases are upgraded to include roll_number.
    columns = [row["name"] for row in conn.execute("PRAGMA table_info(students)").fetchall()]
    if "roll_number" not in columns:
        conn.execute("ALTER TABLE students ADD COLUMN roll_number TEXT")

    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_students_roll_number ON students(roll_number)"
    )

    # Backfill roll_number for any existing records that do not have one.
    students_without_roll = conn.execute(
        "SELECT id FROM students WHERE roll_number IS NULL OR roll_number = '' ORDER BY id ASC"
    ).fetchall()
    try:
        for student in students_without_roll:
            new_roll = generate_next_roll_number(conn)
            conn.execute(
                "UPDATE students SET roll_number = ? WHERE id = ?",
                (new_roll, student["id"]),
            )
    except ValueError:
        conn.close()
        raise

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS semester_marks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            roll_number TEXT NOT NULL,
            semester INTEGER NOT NULL CHECK(semester BETWEEN 1 AND 8),
            subject_name TEXT NOT NULL,
            marks REAL NOT NULL,
            grade TEXT NOT NULL,
            FOREIGN KEY (roll_number) REFERENCES students(roll_number)
        )
        """
    )

    # Prevent duplicate subject entries for the same student and semester.
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sem_marks_unique_subject
        ON semester_marks(roll_number, semester, subject_name)
        """
    )

    conn.commit()
    conn.close()


def read_marks_payload():
    data = request.get_json(silent=True)
    if data is None:
        data = request.form.to_dict()
    return data


def validate_marks_payload(data, require_all_fields=True):
    required_fields = ["roll_number", "semester", "subject_name", "marks", "grade"]

    if require_all_fields:
        for field in required_fields:
            if field not in data or str(data[field]).strip() == "":
                return False, f"Missing field: {field}"

    if "semester" in data:
        try:
            semester = int(data["semester"])
        except (TypeError, ValueError):
            return False, "semester must be an integer between 1 and 8"
        if semester < 1 or semester > 8:
            return False, "semester must be between 1 and 8"

    if "marks" in data:
        try:
            float(data["marks"])
        except (TypeError, ValueError):
            return False, "marks must be a number"

    return True, ""


def normalize_upload_dataframe(dataframe):
    """Normalize uploaded dataframe column names for easier validation."""
    dataframe.columns = [str(column).strip().lower() for column in dataframe.columns]
    return dataframe


def parse_upload_file(uploaded_file):
    """Read CSV/XLSX file and return normalized pandas DataFrame."""
    filename = (uploaded_file.filename or "").lower()

    if filename.endswith(".csv"):
        dataframe = pd.read_csv(uploaded_file)
    elif filename.endswith(".xlsx"):
        dataframe = pd.read_excel(uploaded_file)
    else:
        raise ValueError("Unsupported file type. Please upload .csv or .xlsx")

    return normalize_upload_dataframe(dataframe)


def validate_upload_columns(dataframe):
    """Ensure required columns exist in the uploaded file."""
    missing_columns = [column for column in REQUIRED_UPLOAD_COLUMNS if column not in dataframe.columns]
    if missing_columns:
        raise ValueError(
            "Invalid file format. Missing columns: " + ", ".join(missing_columns)
        )


def build_upload_preview_rows(conn, dataframe):
    """
    Build upload preview rows and split valid/invalid rows before insertion.
    Invalid rows include a message so UI can highlight them.
    """
    preview_rows = []
    valid_rows = []

    # Track duplicates inside file.
    seen_aadhar = set()
    seen_pan = set()

    existing_aadhar = {
        row["aadhar_number"] for row in conn.execute("SELECT aadhar_number FROM students").fetchall()
    }
    existing_pan = {
        row["pan_number"] for row in conn.execute("SELECT pan_number FROM students").fetchall()
    }

    for index, row in dataframe.iterrows():
        row_number = int(index) + 2  # +2 because CSV/XLSX has a header row.

        row_data = {
            "name": "" if pd.isna(row.get("name")) else str(row.get("name")).strip(),
            "department": ""
            if pd.isna(row.get("department"))
            else str(row.get("department")).strip(),
            "year_of_joining": ""
            if pd.isna(row.get("year_of_joining"))
            else str(row.get("year_of_joining")).strip(),
            "tenth_mark": "" if pd.isna(row.get("tenth_mark")) else str(row.get("tenth_mark")).strip(),
            "twelfth_mark": ""
            if pd.isna(row.get("twelfth_mark"))
            else str(row.get("twelfth_mark")).strip(),
            "aadhar_number": ""
            if pd.isna(row.get("aadhar_number"))
            else str(row.get("aadhar_number")).strip(),
            "pan_number": ""
            if pd.isna(row.get("pan_number"))
            else str(row.get("pan_number")).strip().upper(),
        }

        reason = ""
        is_valid = True

        # Skip empty/incomplete rows.
        if any(str(row_data[field]).strip() == "" for field in REQUIRED_UPLOAD_COLUMNS):
            is_valid = False
            reason = "Missing one or more required values"

        if is_valid:
            try:
                row_data["year_of_joining"] = int(float(row_data["year_of_joining"]))
                row_data["tenth_mark"] = float(row_data["tenth_mark"])
                row_data["twelfth_mark"] = float(row_data["twelfth_mark"])
            except ValueError:
                is_valid = False
                reason = "Invalid numeric values in year or marks"

        if is_valid and row_data["aadhar_number"] in existing_aadhar:
            is_valid = False
            reason = "Aadhar already exists in database"

        if is_valid and row_data["pan_number"] in existing_pan:
            is_valid = False
            reason = "PAN already exists in database"

        if is_valid and row_data["aadhar_number"] in seen_aadhar:
            is_valid = False
            reason = "Duplicate Aadhar in uploaded file"

        if is_valid and row_data["pan_number"] in seen_pan:
            is_valid = False
            reason = "Duplicate PAN in uploaded file"

        if is_valid:
            seen_aadhar.add(row_data["aadhar_number"])
            seen_pan.add(row_data["pan_number"])
            valid_rows.append(row_data)

        preview_rows.append(
            {
                "row_number": row_number,
                "data": row_data,
                "is_valid": is_valid,
                "reason": reason,
            }
        )

    return preview_rows, valid_rows


def add_marks_record(conn, data):
    """Insert one semester marks record after validating student existence."""
    roll_number = str(data["roll_number"]).strip()
    semester = int(data["semester"])
    subject_name = str(data["subject_name"]).strip()
    marks = float(data["marks"])
    grade = str(data["grade"]).strip()

    student = conn.execute(
        "SELECT roll_number FROM students WHERE roll_number = ?",
        (roll_number,),
    ).fetchone()
    if student is None:
        return None, 404, "Student roll number not found"

    try:
        cursor = conn.execute(
            """
            INSERT INTO semester_marks (roll_number, semester, subject_name, marks, grade)
            VALUES (?, ?, ?, ?, ?)
            """,
            (roll_number, semester, subject_name, marks, grade),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        return None, 409, "Marks already exist for this roll number, semester, and subject"

    return (
        {
            "id": cursor.lastrowid,
            "roll_number": roll_number,
            "semester": semester,
            "subject_name": subject_name,
            "marks": marks,
            "grade": grade,
        },
        201,
        "Marks added successfully",
    )


def update_marks_record(conn, mark_id, data):
    """Update fields of a semester marks record by id."""
    updatable_fields = ["semester", "subject_name", "marks", "grade"]
    updates = []
    values = []

    for field in updatable_fields:
        if field in data and str(data[field]).strip() != "":
            updates.append(f"{field} = ?")
            if field == "semester":
                values.append(int(data[field]))
            elif field == "marks":
                values.append(float(data[field]))
            else:
                values.append(str(data[field]).strip())

    if not updates:
        return None, 400, "No fields provided for update"

    existing = conn.execute("SELECT id FROM semester_marks WHERE id = ?", (mark_id,)).fetchone()
    if existing is None:
        return None, 404, "Marks record not found"

    values.append(mark_id)
    try:
        conn.execute(
            f"UPDATE semester_marks SET {', '.join(updates)} WHERE id = ?",
            tuple(values),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        return None, 409, "Update would create a duplicate subject entry for this semester"

    updated = conn.execute("SELECT * FROM semester_marks WHERE id = ?", (mark_id,)).fetchone()
    return dict(updated), 200, "Marks updated successfully"


def fetch_marks_for_roll(conn, roll_number):
    """Fetch student and all semester marks for a given roll number."""
    student = conn.execute(
        "SELECT roll_number, name, department, year_of_joining FROM students WHERE roll_number = ?",
        (roll_number,),
    ).fetchone()
    if student is None:
        return None, None

    marks_rows = conn.execute(
        """
        SELECT id, roll_number, semester, subject_name, marks, grade
        FROM semester_marks
        WHERE roll_number = ?
        ORDER BY semester ASC, subject_name ASC
        """,
        (roll_number,),
    ).fetchall()
    return student, marks_rows


def fetch_marks_for_roll_and_semester(conn, roll_number, semester):
    """Fetch marks for one student and one semester."""
    marks_rows = conn.execute(
        """
        SELECT id, roll_number, semester, subject_name, marks, grade
        FROM semester_marks
        WHERE roll_number = ? AND semester = ?
        ORDER BY subject_name ASC
        """,
        (roll_number, semester),
    ).fetchall()
    return marks_rows


def parse_multi_subject_rows(subject_names, marks_list, grades):
    """Validate and normalize multi-row subject data from form arrays."""
    parsed_rows = []
    seen_subjects = set()

    max_len = max(len(subject_names), len(marks_list), len(grades))
    for index in range(max_len):
        subject_name = subject_names[index].strip() if index < len(subject_names) else ""
        marks_value = marks_list[index].strip() if index < len(marks_list) else ""
        grade_value = grades[index].strip() if index < len(grades) else ""

        # Ignore fully empty rows that can appear from an extra blank input row.
        if subject_name == "" and marks_value == "" and grade_value == "":
            continue

        if subject_name == "":
            return None, f"Row {index + 1}: Subject name is required"
        if marks_value == "":
            return None, f"Row {index + 1}: Marks are required"
        if grade_value == "":
            return None, f"Row {index + 1}: Grade is required"

        try:
            numeric_marks = float(marks_value)
        except ValueError:
            return None, f"Row {index + 1}: Marks must be numeric"

        normalized_subject = subject_name.lower()
        if normalized_subject in seen_subjects:
            return None, f"Row {index + 1}: Duplicate subject name in form"
        seen_subjects.add(normalized_subject)

        parsed_rows.append(
            {
                "subject_name": subject_name,
                "marks": numeric_marks,
                "grade": grade_value,
            }
        )

    if not parsed_rows:
        return None, "Please add at least one valid subject row"

    return parsed_rows, ""


def insert_student_from_row(conn, row_data):
    """
    Insert one student record from bulk upload row.
    Returns tuple: (inserted: bool, reason: str)
    """
    required_values = [
        row_data.get("name"),
        row_data.get("department"),
        row_data.get("year_of_joining"),
        row_data.get("tenth_mark"),
        row_data.get("twelfth_mark"),
        row_data.get("aadhar_number"),
        row_data.get("pan_number"),
    ]

    # Skip fully or partially empty rows.
    if any(pd.isna(value) or str(value).strip() == "" for value in required_values):
        return False, "Skipped empty/incomplete row"

    name = str(row_data["name"]).strip()
    department = str(row_data["department"]).strip()
    year_of_joining = int(float(row_data["year_of_joining"]))
    tenth_mark = float(row_data["tenth_mark"])
    twelfth_mark = float(row_data["twelfth_mark"])
    aadhar_number = str(row_data["aadhar_number"]).strip()
    pan_number = str(row_data["pan_number"]).strip().upper()

    duplicate_aadhar = conn.execute(
        "SELECT id FROM students WHERE aadhar_number = ?",
        (aadhar_number,),
    ).fetchone()
    if duplicate_aadhar:
        return False, "Duplicate Aadhar skipped"

    duplicate_pan = conn.execute(
        "SELECT id FROM students WHERE pan_number = ?",
        (pan_number,),
    ).fetchone()
    if duplicate_pan:
        return False, "Duplicate PAN skipped"

    generated_roll = generate_next_roll_number(conn)
    conn.execute(
        """
        INSERT INTO students (
            roll_number,
            name,
            department,
            year_of_joining,
            tenth_mark,
            twelfth_mark,
            aadhar_number,
            pan_number
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            generated_roll,
            name,
            department,
            year_of_joining,
            tenth_mark,
            twelfth_mark,
            aadhar_number,
            pan_number,
        ),
    )

    return True, "Inserted"


def format_marks_value(marks):
    """Return compact display value for numeric marks in pivot cells."""
    marks_number = float(marks)
    if marks_number.is_integer():
        return str(int(marks_number))
    return f"{marks_number:.2f}"


def build_semester_pivot_data(conn, department, batch, semester, view_mode):
    """
    Build pivot-friendly data.
    Rows: students (roll number), Columns: subject names, Values: marks/grade.
    """
    filters = []
    params = []

    if department:
        filters.append("s.department = ?")
        params.append(department)

    if batch:
        filters.append("s.year_of_joining = ?")
        params.append(batch)

    student_query = """
        SELECT s.roll_number, s.name, s.department, s.year_of_joining
        FROM students s
    """
    if filters:
        student_query += " WHERE " + " AND ".join(filters)
    student_query += " ORDER BY s.roll_number ASC"
    selected_students = conn.execute(student_query, params).fetchall()

    subject_query = """
        SELECT DISTINCT sm.subject_name
        FROM semester_marks sm
        JOIN students s ON s.roll_number = sm.roll_number
        WHERE sm.semester = ?
    """
    subject_params = [semester]
    if filters:
        subject_query += " AND " + " AND ".join(filters)
        subject_params.extend(params)
    subject_query += " ORDER BY sm.subject_name ASC"
    subjects = [row["subject_name"] for row in conn.execute(subject_query, subject_params).fetchall()]

    marks_query = """
        SELECT sm.roll_number, sm.subject_name, sm.marks, sm.grade
        FROM semester_marks sm
        JOIN students s ON s.roll_number = sm.roll_number
        WHERE sm.semester = ?
    """
    marks_params = [semester]
    if filters:
        marks_query += " AND " + " AND ".join(filters)
        marks_params.extend(params)
    marks_rows = conn.execute(marks_query, marks_params).fetchall()

    marks_map = {}
    for row in marks_rows:
        if row["roll_number"] not in marks_map:
            marks_map[row["roll_number"]] = {}

        if view_mode == "grades":
            marks_map[row["roll_number"]][row["subject_name"]] = row["grade"]
        else:
            marks_map[row["roll_number"]][row["subject_name"]] = format_marks_value(row["marks"])

    pivot_rows = []
    for student in selected_students:
        row_values = {}
        for subject in subjects:
            row_values[subject] = marks_map.get(student["roll_number"], {}).get(subject, "-")

        pivot_rows.append(
            {
                "roll_number": student["roll_number"],
                "name": student["name"],
                "department": student["department"],
                "year_of_joining": student["year_of_joining"],
                "subject_values": row_values,
            }
        )

    return subjects, pivot_rows


@app.route("/")
def home():
    conn = get_db_connection()

    total_students_row = conn.execute(
        "SELECT COUNT(*) AS total_students FROM students"
    ).fetchone()
    department_counts = conn.execute(
        """
        SELECT department, COUNT(*) AS student_count
        FROM students
        GROUP BY department
        ORDER BY student_count DESC, department ASC
        """
    ).fetchall()
    recent_students = conn.execute(
        "SELECT * FROM students ORDER BY id DESC LIMIT 8"
    ).fetchall()

    conn.close()

    return render_template(
        "home.html",
        total_students=total_students_row["total_students"],
        department_counts=department_counts,
        recent_students=recent_students,
    )


@app.route("/register", methods=["GET", "POST"])
def register_student():
    if request.method == "POST":
        name = request.form["name"]
        department = request.form["department"]
        year_of_joining = request.form["year_of_joining"]
        tenth_mark = request.form["tenth_mark"]
        twelfth_mark = request.form["twelfth_mark"]
        aadhar_number = request.form["aadhar_number"]
        pan_number = request.form["pan_number"]

        conn = get_db_connection()
        generated_roll = None

        # Retry insert if a race condition causes duplicate roll_number.
        for _ in range(5):
            try:
                generated_roll = generate_next_roll_number(conn)
            except ValueError as error:
                conn.close()
                return str(error), 400
            try:
                conn.execute(
                    """
                    INSERT INTO students (
                        roll_number,
                        name,
                        department,
                        year_of_joining,
                        tenth_mark,
                        twelfth_mark,
                        aadhar_number,
                        pan_number
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        generated_roll,
                        name,
                        department,
                        year_of_joining,
                        tenth_mark,
                        twelfth_mark,
                        aadhar_number,
                        pan_number,
                    ),
                )
                break
            except sqlite3.IntegrityError:
                generated_roll = None

        if generated_roll is None:
            conn.close()
            return "Unable to generate a unique roll number. Please try again.", 500

        conn.commit()
        conn.close()

        return redirect(url_for("students_list", registered_roll_number=generated_roll))

    return render_template("register.html")


@app.route("/upload", methods=["GET", "POST"])
def bulk_upload_students():
    """Upload CSV/XLSX, preview rows, and confirm before final insertion."""
    if request.method == "POST":
        action = request.form.get("action", "preview")

        if action == "confirm":
            valid_rows = session.get("bulk_upload_valid_rows", [])
            if not valid_rows:
                return render_template(
                    "upload.html",
                    error_message="No preview data found. Please upload and preview file again.",
                )

            conn = get_db_connection()
            inserted_count = 0
            skipped_count = 0

            try:
                for row_data in valid_rows:
                    inserted, _ = insert_student_from_row(conn, row_data)
                    if inserted:
                        inserted_count += 1
                    else:
                        skipped_count += 1

                conn.commit()
            except ValueError as error:
                conn.rollback()
                conn.close()
                return render_template("upload.html", error_message=str(error))
            except Exception:
                conn.rollback()
                conn.close()
                return render_template(
                    "upload.html",
                    error_message="Upload failed while saving. Please try again.",
                )

            conn.close()
            session.pop("bulk_upload_valid_rows", None)

            return render_template(
                "upload.html",
                success_message=f"{inserted_count} students added successfully",
                skipped_message=f"{skipped_count} rows skipped during save" if skipped_count else "",
            )

        uploaded_file = request.files.get("student_file")

        if uploaded_file is None or uploaded_file.filename.strip() == "":
            return render_template(
                "upload.html",
                error_message="Please choose a CSV or Excel file to upload.",
            )

        try:
            dataframe = parse_upload_file(uploaded_file)
            validate_upload_columns(dataframe)
        except Exception as error:
            return render_template("upload.html", error_message=str(error))

        conn = get_db_connection()
        preview_rows, valid_rows = build_upload_preview_rows(conn, dataframe)
        conn.close()

        session["bulk_upload_valid_rows"] = valid_rows

        valid_count = len(valid_rows)
        invalid_count = len(preview_rows) - valid_count

        return render_template(
            "upload.html",
            preview_rows=preview_rows,
            valid_count=valid_count,
            invalid_count=invalid_count,
            show_confirm_button=valid_count > 0,
        )

    session.pop("bulk_upload_valid_rows", None)
    return render_template("upload.html")


@app.route("/marks", methods=["GET", "POST"])
def marks_management():
    """
    Semester-wise multi-row marks editor for one student at a time.
    """
    if request.method == "POST":
        roll_number = request.form.get("roll_number", "").strip()
        semester_raw = request.form.get("semester", "").strip()
        subject_names = request.form.getlist("subject_name[]")
        marks_list = request.form.getlist("marks[]")
        grades = request.form.getlist("grade[]")

        if roll_number == "":
            return redirect(url_for("marks_management", error_message="Please select roll number"))

        try:
            semester = int(semester_raw)
        except ValueError:
            return redirect(
                url_for(
                    "marks_management",
                    roll_number=roll_number,
                    error_message="Semester must be between 1 and 8",
                )
            )

        if semester < 1 or semester > 8:
            return redirect(
                url_for(
                    "marks_management",
                    roll_number=roll_number,
                    error_message="Semester must be between 1 and 8",
                )
            )

        parsed_rows, error_message = parse_multi_subject_rows(subject_names, marks_list, grades)
        if parsed_rows is None:
            return redirect(
                url_for(
                    "marks_management",
                    roll_number=roll_number,
                    semester=semester,
                    error_message=error_message,
                )
            )

        conn = get_db_connection()
        student = conn.execute(
            "SELECT roll_number FROM students WHERE roll_number = ?",
            (roll_number,),
        ).fetchone()
        if student is None:
            conn.close()
            return redirect(
                url_for("marks_management", error_message="Student roll number not found")
            )

        try:
            # Replace all marks for selected roll number + semester with submitted rows.
            conn.execute(
                "DELETE FROM semester_marks WHERE roll_number = ? AND semester = ?",
                (roll_number, semester),
            )

            for row in parsed_rows:
                conn.execute(
                    """
                    INSERT INTO semester_marks (roll_number, semester, subject_name, marks, grade)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        roll_number,
                        semester,
                        row["subject_name"],
                        row["marks"],
                        row["grade"],
                    ),
                )

            conn.commit()
        except Exception:
            conn.rollback()
            conn.close()
            return redirect(
                url_for(
                    "marks_management",
                    roll_number=roll_number,
                    semester=semester,
                    error_message="Unable to save marks. Please check values and try again.",
                )
            )

        conn.close()
        return redirect(
            url_for(
                "marks_management",
                roll_number=roll_number,
                semester=semester,
                success_message="Marks saved successfully",
            )
        )

    selected_roll_number = request.args.get("roll_number", "").strip()
    selected_semester_raw = request.args.get("semester", "1").strip()
    success_message = request.args.get("success_message", "")
    error_message = request.args.get("error_message", "")

    try:
        selected_semester = int(selected_semester_raw)
    except ValueError:
        selected_semester = 1

    if selected_semester < 1 or selected_semester > 8:
        selected_semester = 1

    conn = get_db_connection()
    students = conn.execute(
        "SELECT roll_number, name, department FROM students ORDER BY id DESC"
    ).fetchall()

    student = None
    marks_rows = []
    if selected_roll_number:
        student, _ = fetch_marks_for_roll(conn, selected_roll_number)
        if student is None:
            error_message = "Student roll number not found"
        else:
            marks_rows = fetch_marks_for_roll_and_semester(
                conn,
                selected_roll_number,
                selected_semester,
            )

    conn.close()

    return render_template(
        "marks.html",
        students=students,
        selected_roll_number=selected_roll_number,
        selected_semester=selected_semester,
        selected_student=student,
        marks_rows=marks_rows,
        success_message=success_message,
        error_message=error_message,
    )


@app.route("/marks/update", methods=["POST"])
def update_marks_form():
    """Handle marks update from HTML form."""
    mark_id = request.form.get("mark_id", "").strip()
    roll_number = request.form.get("roll_number", "").strip()

    if not mark_id.isdigit():
        return redirect(
            url_for(
                "marks_management",
                roll_number=roll_number,
                error_message="Invalid marks record id",
            )
        )

    update_data = {}
    for field in ["semester", "subject_name", "marks", "grade"]:
        value = request.form.get(field, "").strip()
        if value:
            update_data[field] = value

    is_valid, error_message = validate_marks_payload(update_data, require_all_fields=False)
    if not is_valid:
        return redirect(
            url_for("marks_management", roll_number=roll_number, error_message=error_message)
        )

    conn = get_db_connection()
    _, status_code, message = update_marks_record(conn, int(mark_id), update_data)
    conn.close()

    if status_code != 200:
        return redirect(
            url_for("marks_management", roll_number=roll_number, error_message=message)
        )

    return redirect(
        url_for(
            "marks_management",
            roll_number=roll_number,
            success_message="Marks updated successfully",
        )
    )


@app.route("/marks/pivot")
def marks_pivot_table():
    """Show semester marks in pivot table format with filters and marks/grades toggle."""
    selected_department = request.args.get("department", "").strip()
    selected_batch = request.args.get("batch", "").strip()
    selected_semester_raw = request.args.get("semester", "1").strip()
    view_mode = request.args.get("view", "marks").strip().lower()

    if view_mode not in ["marks", "grades"]:
        view_mode = "marks"

    try:
        selected_semester = int(selected_semester_raw)
    except ValueError:
        selected_semester = 1

    if selected_semester < 1 or selected_semester > 8:
        selected_semester = 1

    conn = get_db_connection()
    departments = conn.execute(
        "SELECT DISTINCT department FROM students ORDER BY department ASC"
    ).fetchall()
    batches = conn.execute(
        "SELECT DISTINCT year_of_joining FROM students ORDER BY year_of_joining DESC"
    ).fetchall()

    subjects, pivot_rows = build_semester_pivot_data(
        conn,
        selected_department,
        selected_batch,
        selected_semester,
        view_mode,
    )
    conn.close()

    return render_template(
        "marks_pivot.html",
        departments=departments,
        batches=batches,
        selected_department=selected_department,
        selected_batch=selected_batch,
        selected_semester=selected_semester,
        selected_view_mode=view_mode,
        subjects=subjects,
        pivot_rows=pivot_rows,
    )


@app.route("/students")
def students_list():
    search_roll_number = request.args.get("roll_number", "").strip()
    selected_department = request.args.get("department", "").strip()

    conn = get_db_connection()

    query = "SELECT * FROM students"
    filters = []
    params = []

    if search_roll_number:
        filters.append("roll_number = ?")
        params.append(search_roll_number)

    if selected_department:
        filters.append("department = ?")
        params.append(selected_department)

    if filters:
        query += " WHERE " + " AND ".join(filters)

    query += " ORDER BY id DESC"
    students = conn.execute(query, params).fetchall()

    departments = conn.execute(
        "SELECT DISTINCT department FROM students ORDER BY department ASC"
    ).fetchall()

    conn.close()

    latest_roll_number = request.args.get("registered_roll_number")
    return render_template(
        "students.html",
        students=students,
        latest_roll_number=latest_roll_number,
        departments=departments,
        search_roll_number=search_roll_number,
        selected_department=selected_department,
    )


@app.route("/api/marks", methods=["POST"])
def add_marks():
    data = read_marks_payload()
    is_valid, error_message = validate_marks_payload(data, require_all_fields=True)
    if not is_valid:
        return jsonify({"success": False, "message": error_message}), 400

    conn = get_db_connection()
    record, status_code, message = add_marks_record(conn, data)
    conn.close()

    if status_code != 201:
        return jsonify({"success": False, "message": message}), status_code

    return (
        jsonify(
            {
                "success": True,
                "message": message,
                "data": record,
            }
        ),
        201,
    )


@app.route("/api/marks/<int:mark_id>", methods=["PUT"])
def update_marks(mark_id):
    data = read_marks_payload()
    is_valid, error_message = validate_marks_payload(data, require_all_fields=False)
    if not is_valid:
        return jsonify({"success": False, "message": error_message}), 400

    conn = get_db_connection()
    updated, status_code, message = update_marks_record(conn, mark_id, data)
    conn.close()

    if status_code != 200:
        return jsonify({"success": False, "message": message}), status_code

    return (
        jsonify(
            {
                "success": True,
                "message": message,
                "data": updated,
            }
        ),
        200,
    )


@app.route("/api/marks/roll/<roll_number>", methods=["GET"])
def view_marks_by_roll_number(roll_number):
    conn = get_db_connection()
    student, marks_rows = fetch_marks_for_roll(conn, roll_number)
    if student is None:
        conn.close()
        return jsonify({"success": False, "message": "Student roll number not found"}), 404
    conn.close()

    return (
        jsonify(
            {
                "success": True,
                "student": dict(student),
                "marks": [dict(row) for row in marks_rows],
            }
        ),
        200,
    )


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=10000)(debug=True)

"""Quick upload of student notebooks to Databricks workspace."""
import json, base64
from pathlib import Path

secrets = json.loads((Path(__file__).resolve().parent.parent.parent / "_secrets.json").read_text())
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

w = WorkspaceClient(host=secrets["databricks_host"], token=secrets["databricks_token"])

NOTEBOOKS = {
    "nb1_spark_intro.py": "/Users/gsalu@ncf.edu/_testing/student_nb1_spark_intro",
    "nb2_file_formats.py": "/Users/gsalu@ncf.edu/_testing/student_nb2_file_formats",
    "nb3_sql.py": "/Users/gsalu@ncf.edu/_testing/student_nb3_sql",
}

nb_dir = Path(__file__).parent

try:
    w.workspace.mkdirs("/Users/gsalu@ncf.edu/_testing")
except:
    pass

for filename, db_path in NOTEBOOKS.items():
    local_path = nb_dir / filename
    if not local_path.exists():
        print(f"SKIP: {filename} not found")
        continue
    
    content = local_path.read_bytes()
    content_b64 = base64.b64encode(content).decode("utf-8")
    
    w.workspace.import_(
        path=db_path, content=content_b64,
        format=ImportFormat.SOURCE, language=Language.PYTHON,
        overwrite=True
    )
    print(f"Uploaded: {filename} -> {db_path}")

print("Done! All student notebooks uploaded.")

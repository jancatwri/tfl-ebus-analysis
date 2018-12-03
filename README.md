# TfL e-bus analysis

# Inputs
'parse.py' takes '.gz' files from TfL arrival prediction data
'layovers_run.ipynb' takes '.csv' arrival prediction data. Uses 'layovers_simplified.py' or 'layovers_extended.py'

# Outputs
'parse.py' outputs 'combinedfile.csv' for each day, or outputs a '.csv' given a specified file name for data grouped over multiple days
'layovers_run.ipynb' with 'layovers_simplified.py' outputs '.csv' bus graph data (timetable) with generic start and end points. 'layovers_extended.py' outputs '.csv' bus graph data with Naptan start and end points, and '.csv' estimated battery consumption data.

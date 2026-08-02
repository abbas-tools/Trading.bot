import sqlite3

conn = sqlite3.connect("trading_bot.db")
cursor = conn.cursor()
try:
  cursor.execute("ALTER TABLE performance ADD COLUMN chart_url TEXT;")
  conn.commit()
  print("Column 'chart_url' added successfully!")
except sqlite3.OperationalError as e:
  print("Note:", e)
conn.close()

import ast, sys
try:
    src = open("app.py", encoding="utf-8").read()
    ast.parse(src)
    print("SYNTAX OK")
except SyntaxError as e:
    print("SYNTAX ERROR:", e)
    sys.exit(1)


import ast
with open('app.py', encoding='utf-8') as f:
    src = f.read()
try:
    ast.parse(src)
    print('OK')
except SyntaxError as e:
    print(f'SyntaxError line {e.lineno}: {e.msg}')
    print(e.text)


import ast, sys
try:
    src = open(r'C:\Users\ourie\PycharmProjects\my-portfolio-os\app.py', encoding='utf-8').read()
    ast.parse(src)
    print('SYNTAX OK')
except SyntaxError as e:
    print(f'SYNTAX ERROR: {e}')
    sys.exit(1)


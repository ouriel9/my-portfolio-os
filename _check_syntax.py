import sys
try:
    with open(r'C:\Users\ourie\PycharmProjects\my-portfolio-os\app.py', encoding='utf-8') as f:
        source = f.read()
    compile(source, 'app.py', 'exec')
    print('SYNTAX OK - total lines:', source.count('\n')+1)
except SyntaxError as e:
    print(f'SYNTAX ERROR: {e}')
    sys.exit(1)


content = open('app.py', encoding='utf-8').read()
print('theme="streamlit" count:', content.count('theme="streamlit"'))
print('st.plotly_chart count:', content.count('st.plotly_chart'))
print('Reports tab:', 'tr("Reports"' in content or '"Reports"' in content)
print('margin l=150:', 'l=150' in content)


import re

with open('app.py', encoding='utf-8') as f:
    content = f.read()

# Add theme="streamlit" to st.plotly_chart calls that have use_container_width=True
# Pattern: st.plotly_chart(..., use_container_width=True) -> add theme="streamlit"
# Only if theme= is not already present

def add_theme(m):
    s = m.group(0)
    if 'theme=' in s:
        return s
    # Insert theme="streamlit" before the closing )
    s = s.rstrip(')')
    s += ', theme="streamlit")'
    return s

# Match st.plotly_chart( ... use_container_width=True) possibly with trailing whitespace
pattern = r'st\.plotly_chart\([^)]+use_container_width=True\)'
new_content = re.sub(pattern, add_theme, content)

# Also handle use_container_width=False
pattern2 = r'st\.plotly_chart\([^)]+use_container_width=False\)'
new_content = re.sub(pattern2, add_theme, new_content)

count_before = content.count('theme="streamlit"')
count_after = new_content.count('theme="streamlit"')
print(f'theme="streamlit" before: {count_before}, after: {count_after}')

with open('app.py', 'w', encoding='utf-8') as f:
    f.write(new_content)

print('Done')


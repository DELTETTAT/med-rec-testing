import re
from collections import defaultdict

# Read the file
with open(r"C:\Users\pc\Desktop\unify.txt", "r", encoding="utf-8") as f:
    content = f.read()

# Regex to capture fieldName: "group.field"
pattern = re.compile(r'fieldName:\s*"(.*?)"')

groups = defaultdict(list)

for match in pattern.findall(content):
    if "." in match:
        group, field = match.split(".", 1)
        groups[group].append(field)

# Convert defaultdict to normal dict
groups = dict(groups)

# Print results
for group, fields in groups.items():
    print(f"{group}: {fields}")

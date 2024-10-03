from pathlib import Path

files = list((Path(__file__).parent / "dataset").glob("**/*.csv"))

bad_char = "�"

# This stupid script fixes some files that have invalid utf-8 chars.
# Mainly chars with accents, I just remove it cause I would have to
# go through each occurence an I have no time for that.

bad_files = []
for file in files:
    t = file.read_text("utf-8", errors="replace")
    if bad_char not in t:
        continue

    t = t.replace(bad_char, "")
    with file.open("w") as f:
        f.write(t)

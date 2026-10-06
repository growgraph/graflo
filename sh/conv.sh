#!/bin/bash

# convert pdf to png
# === Usage Help ===
if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <input_dir> <output_dir> [dpi]"
  exit 1
fi

INPUT_DIR="$1"
OUTPUT_DIR="$2"
DPI="${3:-300}"  # Default DPI = 300 if not specified

# === Check input directory ===
if [ ! -d "$INPUT_DIR" ]; then
  echo "Error: Input directory '$INPUT_DIR' does not exist."
  exit 1
fi

# === Create output directory if it doesn't exist ===
mkdir -p "$OUTPUT_DIR"

# === Loop over all PDF files ===
shopt -s nullglob
for pdf_file in "$INPUT_DIR"/*.pdf; do
  filename=$(basename -- "$pdf_file")
  name="${filename%.*}"

  echo "Processing: $filename"

  page_count=$(pdfinfo "$pdf_file" | grep "Pages:" | awk '{print $2}')

  if [[ "$page_count" -eq 1 ]]; then
      # Single page: direct conversion to .png
      pdftoppm -png -r "$DPI" "$pdf_file" "$OUTPUT_DIR/${name}"
      # Rename from name-1.png to name.png
      mv "$OUTPUT_DIR/${name}-1.png" "$OUTPUT_DIR/${name}.png"
  else
      # Multiple pages: keep page numbering
      pdftoppm -png -r "$DPI" "$pdf_file" "$OUTPUT_DIR/${name}_page"
  fi

done
shopt -u nullglob

echo "All done."
